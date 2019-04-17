package io.simplesource.saga.action.app;

import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.properties.PropertiesBuilder;
import lombok.Value;

/**
 * An ActionAppContext represents the context with all the details required to set up an
 * action processor stream.
 *
 * @param <A> The action command type (shared across all actions)
 *
 * @see ActionApp
 * @see io.simplesource.saga.action.app.ActionProcessorBuildStep
 */
@Value(staticConstructor = "of")
public class ActionAppContext<A> {
    public final ActionSpec<A> actionSpec;
    public final PropertiesBuilder.BuildSteps propertiesBuilder;
}
