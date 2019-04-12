package io.simplesource.saga.action.app;

import io.simplesource.saga.shared.streams.StreamBuildSpec;

/**
 * Represents a step in the process of building an action processor
 * app.
 *
 * An {@code ActionProcessorBuildStep} is required for each action processor we add to the application.
 *
 * @param <A> The action command type (shared across all actions)
 *
 * @see io.simplesource.saga.action.ActionApp
 */
@FunctionalInterface
public interface ActionProcessorBuildStep<A> {
    StreamBuildSpec applyStep(ActionAppContext<A> context);
}