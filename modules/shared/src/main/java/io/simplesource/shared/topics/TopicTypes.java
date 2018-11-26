package io.simplesource.shared.topics;

import com.google.common.collect.Lists;

import java.util.List;

class TopicTypes {
    static class CommandTopic {
        String request  = "command_request";
        String response = "command_response";

        List<String> all = Lists.newArrayList(request, response);
    }

    static class ActionTopic {
        String request            = "action_request";
        String requestUnprocessed = "action_request_unprocessed";
        String response           = "action_response";
        List<String> all = Lists.newArrayList(request, response);
    }

    static class SagaTopic {
        String request          = "saga_request";
        String response         = "saga_response";
        String responseTopicMap = "saga_response_topic_map";
        String state            = "saga_state";
        String stateTransition  = "saga_state_transition";

        List<String> all = Lists.newArrayList(request, response, responseTopicMap, state, stateTransition);
        List<String> client = Lists.newArrayList(request, response);
    }
}
