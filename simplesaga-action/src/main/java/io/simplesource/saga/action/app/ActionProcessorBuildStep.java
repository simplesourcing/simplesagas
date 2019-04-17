package io.simplesource.saga.action.app;

import io.simplesource.saga.shared.app.StreamBuildSpec;

/**
 * Represents a step in the process of building an action processor
 * app.
 *
 * An {@code ActionProcessorBuildStep} is required for each action processor we add to the application.
 *
 * @param <A> The action command type (shared across all actions)
 *
 * @see ActionApp
 */
@FunctionalInterface
public interface ActionProcessorBuildStep<A> {
    StreamBuildSpec applyStep(ActionAppContext<A> context);
}
