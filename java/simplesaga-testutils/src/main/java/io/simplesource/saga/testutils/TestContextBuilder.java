package io.simplesource.saga.testutils;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

public class TestContextBuilder {
    private final StreamContext context;
    private Properties props;

    private TestContextBuilder(StreamContext context, Properties props) {
        this.context = context;
        this.props = props;
    }

    public static TestContextBuilder of(StreamContext context) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app_id_" + UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return of(context, props);
    }

    public static TestContextBuilder of(StreamContext context, Properties props) {
        return new TestContextBuilder(context, props);
    }

    public TestContextBuilder addSteps(Consumer<StreamContext> streamBuildStep) {
        streamBuildStep.accept(context);
        return this;
    }

    public TestContextBuilder withProps(Properties props) {
        this.props = props;
        return this;
    }

    public TestContext build() {
        Topology topology = context.getBuilder().build();
        TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);
        return new TestContext(testDriver);
    }
}
