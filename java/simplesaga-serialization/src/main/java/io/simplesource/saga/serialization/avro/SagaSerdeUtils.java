package io.simplesource.saga.serialization.avro;

import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.serialization.avro.generated.AvroSagaError;
import org.apache.avro.generic.GenericArray;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SagaSerdeUtils {

    static <R, T> Result<SagaError, T> getSagaResult(Object aRes, Function<R, T> successTransformer) {
        // TODO: remove the casting
        Result<SagaError, T> result;
        if (aRes instanceof GenericArray) {
            GenericArray<Object> v = (GenericArray) aRes;
            Stream<AvroSagaError> avroErrors = v.stream()
                    .map(x -> (AvroSagaError) x)
                    .filter(Objects::nonNull);
            result = getSagaError(avroErrors);
        } else {
            R tRes = (R)aRes;
            if (tRes != null) {
                result = Result.success(successTransformer.apply(tRes));
            } else {
                result = Result.failure(SagaError.of(SagaError.Reason.InternalError, "Serialization error"));
            }
        }
        return result;
    }

    static <T> Result<SagaError, T> getSagaError(Stream<AvroSagaError> aRes) {
        return Result.failure(NonEmptyList.fromList(
                aRes
                        .map(ae -> SagaError.of(SagaError.Reason.valueOf(ae.getReason()), ae.getMessage()))
                        .collect(Collectors.toList()))
                .orElse(NonEmptyList.of(
                        SagaError.of(SagaError.Reason.InternalError, "Serialization error"))));
    }

}
