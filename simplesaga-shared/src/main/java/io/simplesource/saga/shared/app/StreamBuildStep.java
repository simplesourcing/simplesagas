package io.simplesource.saga.shared.app;

@FunctionalInterface
public interface StreamBuildStep<I> {
    StreamBuildSpec applyStep(StreamBuildContext<I> context);
}
