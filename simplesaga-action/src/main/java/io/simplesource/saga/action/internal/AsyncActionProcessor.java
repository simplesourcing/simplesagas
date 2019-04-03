package io.simplesource.saga.action.internal;

import io.simplesource.data.Result;
import io.simplesource.saga.action.async.AsyncContext;
import io.simplesource.saga.model.messages.UndoCommand;
import io.simplesource.saga.model.serdes.TopicSerdes;
import io.simplesource.saga.action.async.AsyncSpec;
import io.simplesource.saga.action.async.Callback;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.saga.SagaId;
import io.simplesource.saga.shared.topics.TopicTypes;
import lombok.Value;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

final class AsyncActionProcessor {

    @Value
    private static class ResultGeneration<A, K, R> {
        @Value
        static class ToTopic<K, R> {
            public final String topicName;
            public final TopicSerdes<K, R> outputSerdes;
        }

        public final K key;
        public final R result;
        public final Optional<ToTopic<K, R>> toTopic;
        public final Optional<UndoCommand<A>> undoCommand;
    }

    static <A, D, K, O, R> void processRecord(
            AsyncContext<A, D, K, O, R> asyncContext,
            SagaId sagaId, ActionRequest<A> request,
            AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher,
            Function<TopicSerdes<K, R>, AsyncPublisher<K, R>> outputPublisher) {
        AsyncSpec<A, D, K, O, R> asyncSpec = asyncContext.asyncSpec;
        Result<Throwable, D> decodedInputResult = tryWrap(() ->
                asyncSpec.inputDecoder.apply(request.actionCommand.command));

        AtomicBoolean completed = new AtomicBoolean(false);
        Function<D, Callback<O>> callbackProvider = input -> result -> {
            if (completed.compareAndSet(false, true)) {
                Result<Throwable, Optional<ResultGeneration<A, K, R>>> resultWithOutput = tryWrap(() ->
                        result.flatMap(output -> {
                            Optional<Result<Throwable, ResultGeneration<A, K, R>>> optResultGen =
                                    asyncSpec.resultSpec.flatMap(rSpec -> {
                                        K outputKey = rSpec.keyMapper.apply(input);

                                        Optional<Result<Throwable, ResultGeneration<A, K, R>>> resultGeneration = rSpec.outputMapper.apply(output).map(t -> t.map(r -> {
                                            Optional<UndoCommand<A>> undo = rSpec.undoFunction == null || request.isUndo ?
                                                    Optional.empty() :
                                                    rSpec.undoFunction.apply(input, outputKey, r);

                                            Optional<ResultGeneration.ToTopic<K, R>> toTopic = rSpec.outputSerdes.map(outputSerdes ->
                                                    new ResultGeneration.ToTopic<>(
                                                            asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_OUTPUT),
                                                            outputSerdes));

                                            return new ResultGeneration<>(outputKey, r, toTopic, undo);
                                        }));

                                        return resultGeneration;
                                    });

                            // this is just `sequence` in FP - swapping Result and Option
                            Optional<Result<Throwable, Optional<ResultGeneration<A, K, R>>>> y = optResultGen.map(r -> r.fold(Result::failure, r0 -> Result.success(Optional.of(r0))));
                            return y.orElseGet(() -> Result.success(Optional.empty()));
                        }));

                resultWithOutput.ifSuccessful(resultGenOpt ->
                        resultGenOpt.ifPresent(rg -> {
                            rg.toTopic.ifPresent(toTopic -> {
                                AsyncPublisher<K, R> publisher = outputPublisher.apply(toTopic.outputSerdes);
                                publisher.send(toTopic.topicName, rg.key, rg.result);
                            });
                        }));

                Result<Throwable, Optional<UndoCommand<A>>> e = resultWithOutput.map(rg -> rg.flatMap(r -> r.undoCommand));
                publishActionResult(asyncContext, sagaId, request, responsePublisher, e);
            }
        };

        if (decodedInputResult.isFailure()) {
            publishActionFailure(asyncContext, sagaId, request, responsePublisher, decodedInputResult.failureReasons().get().head());
        } else {
            D decodedInput = decodedInputResult.getOrElse(null);

            Callback<O> callback = callbackProvider.apply(decodedInput);

            asyncSpec.timeout.ifPresent(tmOut -> {
                        asyncContext.executor.schedule(() -> {
                            if (completed.compareAndSet(false, true)) {
                                TimeoutException timeout = new TimeoutException("Timeout after " + tmOut.toString());
                                Result<Throwable, O> timeoutResult = Result.failure(timeout);
                                callback.complete(timeoutResult);
                                publishActionFailure(asyncContext, sagaId, request, responsePublisher, timeout);
                            }
                        }, tmOut.toMillis(), TimeUnit.MILLISECONDS);
                    }
            );
            asyncContext.executor.execute(() -> {
                try {
                    asyncSpec.asyncFunction.accept(decodedInput, callback);
                } catch (Throwable e) {
                    publishActionFailure(asyncContext, sagaId, request, responsePublisher, e);
                }
            });
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

    private static <A, D, K, O, R> void publishActionFailure(
            AsyncContext<A, D, K, O, R> asyncContext,
            SagaId sagaId,
            ActionRequest<A> request,
            AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher,
            Throwable failure) {
        publishActionResult(
                asyncContext,
                sagaId,
                request,
                responsePublisher,
                Result.failure(failure));
    }

    private static <A, D, K, O, R> void publishActionResult(
            AsyncContext<A, D, K, O, R> asyncContext,
            SagaId sagaId,
            ActionRequest<A> request,
            AsyncPublisher<SagaId, ActionResponse<A>> responsePublisher,
            Result<Throwable, Optional<UndoCommand<A>>> result) {

        // TODO: capture timeout exception as SagaError.Reason.Timeout
        Result<SagaError, Optional<UndoCommand<A>>> resultWithUndo = result.fold(es -> Result.failure(
                SagaError.of(SagaError.Reason.InternalError, es.head())),
                Result::success);

        ActionResponse<A> actionResponse = ActionResponse.of(request.sagaId,
                request.actionId,
                request.actionCommand.commandId,
                resultWithUndo);

        responsePublisher.send(asyncContext.actionTopicNamer.apply(TopicTypes.ActionTopic.ACTION_RESPONSE),
                sagaId,
                actionResponse);
    }
}
