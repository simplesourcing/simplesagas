package io.simplesource.saga.testutils;

public interface RecordPublisher<K, V> {
    void publish(K key, V value);
}
