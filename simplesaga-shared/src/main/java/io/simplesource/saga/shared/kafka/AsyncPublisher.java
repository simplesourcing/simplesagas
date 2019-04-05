package io.simplesource.saga.shared.kafka;

@FunctionalInterface
public interface AsyncPublisher<K, V> {
    void send(String topic, K key, V value);
}
