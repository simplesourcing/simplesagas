package io.simplesource.saga.action.common;

import java.util.UUID;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.specs.ActionProcessorSpec;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class ActionProducer {

    static public <A> void actionResponse(ActionProcessorSpec<A> actionProcessorSpec,
                                          TopicNamer topicNamer,
                                          KStream<UUID, ActionResponse>... responses) {

        for (KStream<UUID, ActionResponse> response : responses) {
            response.to(
                    topicNamer.apply(TopicTypes.ActionTopic.response),
                    Produced.with(actionProcessorSpec.serdes.uuid(), actionProcessorSpec.serdes.response()));
        }
    }


    static public <A> void actionRequest(ActionProcessorSpec<A> actionSpec,
                                         TopicNamer topicNamer,
                                         KStream<UUID, ActionRequest<A>> request,
                                         boolean unprocessed) {
        request.to(topicNamer.apply(unprocessed ? TopicTypes.ActionTopic.requestUnprocessed : TopicTypes.ActionTopic.request),
                Produced.with(actionSpec.serdes.uuid(), actionSpec.serdes.request()));
    }
}
