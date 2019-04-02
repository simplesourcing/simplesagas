package io.simplesource.saga.shared.topics;

import java.util.Map;

import static io.simplesource.saga.shared.constants.Constants.ACTION_TOPIC_BASE;

public class TopicUtils {
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
