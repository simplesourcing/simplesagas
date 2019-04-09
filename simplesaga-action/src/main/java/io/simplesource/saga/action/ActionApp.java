package io.simplesource.saga.action;

import io.simplesource.saga.action.app.ActionAppContext;
import io.simplesource.saga.action.app.ActionProcessor;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionSpec;
import io.simplesource.saga.shared.kafka.PropertiesBuilder;
import io.simplesource.saga.shared.streams.*;

import java.time.Duration;
import java.util.Properties;

public class ActionApp<A> {

    StreamApp<ActionSpec<A>> streamApp;

    private ActionApp(ActionSpec<A> streamAppInput) {
        streamApp = new StreamApp<>(streamAppInput);
    }

    public static <A> ActionApp<A> of(ActionSerdes<A> serdes) {
        return new ActionApp<>(ActionSpec.of(serdes));
    }

    public ActionApp<A> withActionProcessor(ActionProcessor<A> processorBuildStep) {
        streamApp.withBuildStep(a ->
                processorBuildStep.applyStep(ActionAppContext.of(a.appInput, a.properties)));
        return this;
    }

    public StreamBuildResult build(PropertiesBuilder.BuildSteps properties) {
        return streamApp.build(properties);
    }

    public void run(StreamAppConfig appConfig) {
        streamApp.run(appConfig);
    }

    public void run(PropertiesBuilder.BuildSteps properties) {
        streamApp.run(properties);
    }
}
