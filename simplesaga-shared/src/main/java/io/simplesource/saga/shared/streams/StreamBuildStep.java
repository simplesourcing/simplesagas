package io.simplesource.saga.shared.streams;

public interface StreamBuildStep<I> {
    StreamBuildSpec applyStep(StreamBuildContext<I> context);
}
