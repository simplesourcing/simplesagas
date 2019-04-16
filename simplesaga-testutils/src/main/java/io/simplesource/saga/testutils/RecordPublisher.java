package io.simplesource.saga.testutils;

@FunctionalInterface
public interface RecordPublisher<K, V> {
    void publish(K key, V value);
}
