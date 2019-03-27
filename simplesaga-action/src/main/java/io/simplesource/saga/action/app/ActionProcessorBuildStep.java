package io.simplesource.saga.action.app;

import io.simplesource.saga.shared.streams.StreamBuildSpec;

public interface ActionProcessorBuildStep<A> {
    StreamBuildSpec applyStep(ActionProcessorAppContext<A> context);
}
