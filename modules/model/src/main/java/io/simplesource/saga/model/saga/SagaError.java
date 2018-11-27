package io.simplesource.saga.model.saga;

import io.simplesource.data.NonEmptyList;
import lombok.Value;

@Value
public class SagaError {
    public final NonEmptyList<String> messages;
    // public final A command;
}
