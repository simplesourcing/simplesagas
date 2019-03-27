package io.simplesource.saga.action.app;

import io.simplesource.saga.shared.streams.StreamBuildSpec;

public interface ActionProcessor<A> {
    StreamBuildSpec applyStep(ActionAppContext<A> context);
}
