package io.simplesource.saga.shared.topics;

import io.simplesource.saga.shared.utils.Lists;

import java.util.List;

public class TopicTypes {


    public static class CommandTopic {
        public static String COMMAND_REQUEST = "command_request";
        public static String COMMAND_RESPONSE = "command_response";

        public static List<String> all = Lists.of(COMMAND_REQUEST, COMMAND_RESPONSE);
    }

    public static class ActionTopic {
        public static String ACTION_REQUEST = "action_request";
        public static String ACTION_REQUEST_UNPROCESSED = "action_request_unprocessed";
        public static String ACTION_RESPONSE = "action_response";
        public static List<String> all = Lists.of(ACTION_REQUEST, ACTION_RESPONSE);
    }

    public static class SagaTopic {
        public static String SAGA_BASE_NAME = "saga";
        public static String SAGA_REQUEST = "saga_request";
        public static String SAGA_RESPONSE = "saga_response";
        public static String SAGA_RESPONSE_TOPIC_MAP = "saga_response_topic_map";
        public static String SAGA_STATE = "saga_state";
        public static String SAGA_STATE_TRANSITION = "saga_state_transition";

        public static List<String> all = Lists.of(SAGA_REQUEST, SAGA_RESPONSE, SAGA_RESPONSE_TOPIC_MAP, SAGA_STATE, SAGA_STATE_TRANSITION);
        public static List<String> client = Lists.of(SAGA_REQUEST, SAGA_RESPONSE);
    }
}
