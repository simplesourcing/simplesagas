package io.simplesource.saga.shared.topics;

import java.util.Map;

public class TopicUtils {
    private static final String ACTION_TOPIC_BASE = "saga_action-";

    public static String actionTopicBaseName(String actionType) {
        return ACTION_TOPIC_BASE + actionType.toLowerCase();
    }

    public static TopicNamer topicNamerOverride(TopicNamer baseNamer, Map<String, String> overrides) {
        return name -> {
            String override = overrides.get(name);
            if (override != null) return override;
            return baseNamer.apply(name);
        };
    }
}
