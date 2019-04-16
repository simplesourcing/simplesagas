package io.simplesource.saga.action.internal;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;

final class StreamUtils {
    static <K, V> ForeachAction<K, V> logValues(Logger logger, String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k, v);
    }
}
