package io.simplesource.saga.action.internal;

@FunctionalInterface
public interface AsyncPublisher<K, V> {
    void send(String topic, K key, V value);
}
