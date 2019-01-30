package io.simplesource.saga.serialization.utils;

import lombok.Value;
import org.apache.kafka.streams.TopologyTestDriver;

@Value
public class TestContext {
    private final TopologyTestDriver testDriver;
}

