package io.simplesource.saga.shared.topics;

import static io.simplesource.saga.shared.constants.Constants.ACTION_TOPIC_BASE;

public class TopicUtils {
    public static String actionTopicBaseName(String actionType) {
        return ACTION_TOPIC_BASE + actionType.toLowerCase();
    }
}
