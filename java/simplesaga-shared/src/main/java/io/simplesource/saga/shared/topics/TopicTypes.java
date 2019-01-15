package io.simplesource.saga.shared.topics;

import java.util.List;

public class TopicTypes {
    public static class CommandTopic {
        public static String request  = "command_request";
        public static String response = "command_response";

        public static List<String> all = List.of(request, response);
    }

    public static class ActionTopic {
        public static String request            = "action_request";
        public static String requestUnprocessed = "action_request_unprocessed";
        public static String response           = "action_response";
        public static List<String> all = List.of(request, response);
    }

    public static class SagaTopic {
        public static String request          = "saga_request";
        public static String response         = "saga_response";
        public static String responseTopicMap = "saga_response_topic_map";
        public static String state            = "saga_state";
        public static String stateTransition  = "saga_state_transition";

        public static List<String> all = List.of(request, response, responseTopicMap, state, stateTransition);
        public static List<String> client = List.of(request, response);
    }
}
