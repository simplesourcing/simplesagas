package io.simplesource.saga.action.internal;

import io.simplesource.data.Result;
import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.action.async.AsyncSerdes;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.action.async.Callback;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.shared.topics.TopicTypes;
import lombok.Value;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

final class AsyncActionProcessor {

    @Value
    private static class ResultGeneration<K, R> {
        public final K key;
        public final String topicName;
        public final AsyncSerdes<K, R> outputSerdes;
        public final R result;
    }

    public static <A, I, K, O, R> void processRecord(
            AsyncContext<A, I, K, O, R> asyncContext,
            UUID sagaId, ActionRequest<A> request,
            AsyncPublisher<UUID, ActionResponse> responsePublisher,
            Function<AsyncSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher) {
        AsyncSpec<A, I, K, O, R> asyncSpec = asyncContext.asyncSpec;
        Result<Throwable, I> decodedInput = tryWrap(() ->
                asyncSpec.inputDecoder.apply(request.actionCommand.command));

        AtomicBoolean completed = new AtomicBoolean(false);
        Function<I, Callback<O>> cpb = input -> result -> {
            if (completed.compareAndSet(false, true)) {
                Result<Throwable, Optional<ResultGeneration<K, R>>> resultWithOutput = tryWrap(() ->
                        result.flatMap(output -> {
                            Optional<Result<Throwable, ResultGeneration<K, R>>> x =
                                    asyncSpec.outputSpec.flatMap(oSpec -> {
                                        K outputKey = oSpec.keyMapper.apply(input);
                                        Optional<String> topicNameOpt = oSpec.topicName.apply(input);
                                        return topicNameOpt.flatMap(tName ->
                                                oSpec.outputDecoder.apply(output)).map(t ->
                                                t.map(r -> new ResultGeneration<>(outputKey, topicNameOpt.get(), oSpec.serdes, r)));
                                    });

                            // this is just `sequence` in FP - swapping Result and Option
                            Optional<Result<Throwable, Optional<ResultGeneration<K, R>>>> y = x.map(r -> r.fold(Result::failure, r0 -> Result.success(Optional.of(r0))));
                            return y.orElseGet(() -> Result.success(Optional.empty()));
                        }));

                resultWithOutput.ifSuccessful(resultGenOpt ->
                        resultGenOpt.ifPresent(rg -> {
                            AsyncPublisher<K, R> publisher = outputPublisher.apply(rg.outputSerdes);
                            publisher.send(rg.topicName, rg.key, rg.result);
                        }));

                publishActionResult(asyncContext, sagaId, request, responsePublisher, resultWithOutput);
            }
        };

        if (decodedInput.isFailure()) {
            publishActionResult(asyncContext, sagaId, request, responsePublisher, decodedInput);
        } else {
            I inputWithKey = decodedInput.getOrElse(null);

            Callback<O> callback;
            try {
                callback = cpb.apply(inputWithKey);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            asyncSpec.timeout.ifPresent(tmOut -> {
                        asyncContext.executor.schedule(() -> {
                            if (completed.compareAndSet(false, true)) {
                                Result<Throwable, O> timeoutResult = Result.failure(new TimeoutException("Timeout after " + tmOut.toString()));
                                callback.complete(timeoutResult);
                                publishActionResult(asyncContext, sagaId, request, responsePublisher, timeoutResult);
                            }
                        }, tmOut.toMillis(), TimeUnit.MILLISECONDS);
                    }
            );
            asyncContext.executor.execute(() -> {
                try {
                    asyncSpec.asyncFunction.accept(inputWithKey, callback);
                } catch (Throwable e) {
                    Result<Throwable, O> failure = Result.failure(e);
                    publishActionResult(asyncContext, sagaId, request, responsePublisher, failure);
                }
            });
        }
    }

    // execute lazy code and wraps exceptions in a result
    private static <X> Result<Throwable, X> tryPure(Supplier<X> xSupplier) {
        try {
            return Result.success(xSupplier.get());
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    // evaluates code returning a Result that may throw an exception,
    // and turns it into a Result that is guaranteed not to throw
    // (i.e. absorbs exceptions into the failure mode)
    private static <X> Result<Throwable, X> tryWrap(Supplier<Result<Throwable, X>> xSupplier) {
        try {
            return xSupplier.get();
        } catch (Throwable e) {
            return Result.failure(e);
        }
    }

    private static <A, I, K, O, R> void publishActionResult(
            AsyncContext<A, I, K, O, R> asyncContext,
            UUID sagaId,
            ActionRequest<A> request,
            AsyncPublisher<UUID, ActionResponse> responsePublisher,
            Result<Throwable, ?> result) {

        // TODO: capture timeout exception as SagaError.Reason.Timeout
        Result<SagaError, Boolean> booleanResult = result.fold(es -> Result.failure(
                SagaError.of(SagaError.Reason.InternalError, es.head())),
                r -> Result.success(true));

        ActionResponse actionResponse = new ActionResponse(request.sagaId,
                request.actionId,
                request.actionCommand.commandId,
                booleanResult);

        responsePublisher.send(asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.response),
                sagaId,
                actionResponse);
    }
}
