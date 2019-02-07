package io.simplesource.saga.action.common;

import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicNamer;
import io.simplesource.saga.shared.topics.TopicTypes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.UUID;

public class ActionProducer {

    static public <A> void actionResponse(ActionSerdes<A> actionSerdes,
                                          TopicNamer topicNamer,
                                          KStream<UUID, ActionResponse>... responses) {

        for (KStream<UUID, ActionResponse> response : responses) {
            response.to(
                    topicNamer.apply(TopicTypes.ActionTopic.response),
                    Produced.with(actionSerdes.uuid(), actionSerdes.response()));
        }
    }


    static public <A> void actionRequest(ActionSerdes<A> actionSerdes,
                                         TopicNamer topicNamer,
                                         KStream<UUID, ActionRequest<A>> request,
                                         boolean unprocessed) {
        request.to(topicNamer.apply(unprocessed ? TopicTypes.ActionTopic.requestUnprocessed : TopicTypes.ActionTopic.request),
                Produced.with(actionSerdes.uuid(), actionSerdes.request()));
    }
}
