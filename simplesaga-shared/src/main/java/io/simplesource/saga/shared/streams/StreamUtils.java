package io.simplesource.saga.shared.streams;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;

public final class StreamUtils {
    public static <K, V> ForeachAction<K, V> logValues(Logger logger, String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k, v);
    }
}
