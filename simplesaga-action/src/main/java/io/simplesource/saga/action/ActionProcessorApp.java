package io.simplesource.saga.action;

import io.simplesource.saga.action.app.ActionProcessorAppContext;
import io.simplesource.saga.action.app.ActionProcessorBuildStep;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.streams.*;

import java.util.Properties;

public class ActionProcessorApp<A> {

    StreamApp<ActionProcessorSpec<A>> streamApp;

    public ActionProcessorApp(ActionProcessorSpec<A> streamAppInput) {
        streamApp = new StreamApp<>(streamAppInput);
    }

    public static <A> ActionProcessorApp<A> of(ActionSerdes<A> serdes) {
        return new ActionProcessorApp<>(ActionProcessorSpec.of(serdes));
    }

    public ActionProcessorApp<A> withActionProcessor(ActionProcessorBuildStep<A> buildStep) {
        streamApp.withBuildStep(a ->
                buildStep.applyStep(ActionProcessorAppContext.of(a.appInput, a.properties)));
        return this;
    }

    public StreamBuildResult build(Properties properties) {
        return streamApp.build(properties);
    }

    public void run(StreamAppConfig appConfig) {
        streamApp.run(appConfig);
    }
}
