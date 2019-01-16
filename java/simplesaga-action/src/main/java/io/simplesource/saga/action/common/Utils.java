package io.simplesource.saga.action.common;

import org.apache.kafka.streams.kstream.ForeachAction;
import org.slf4j.Logger;

public class Utils {
  static public <K, V> ForeachAction<K, V> logValues(Logger logger, String prefix) {
        return (k, v) -> logger.info("{}: {}={}", prefix, k, v);
    }
}
