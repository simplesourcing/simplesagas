package io.simplesource.saga.action.internal;

import java.io.Closeable;

/**
 * AsyncPipe should be closed when the application shuts down.
 */
@FunctionalInterface
public interface AsyncPipe extends Closeable {
    void close();
}
