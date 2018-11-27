package io.simplesource.saga.shared.utils;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.saga.shared.topics.TopicCreation;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;


@Value
public class StreamAppConfig {
    public final String appId;
    public final String bootstrapServers;

    public static Properties getConfig(StreamAppConfig appConfig) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.appId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return config;
    }

    CreateTopicsResult addMissingTopics(AdminClient adminClient, List<TopicCreation> topics){
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            Stream<NewTopic> newTopics = topics.stream()
                    .filter(t -> !existingTopics.contains(t.topicName))
                    .map(t -> {
                        TopicSpec spec = t.topicSpec;
                        return new NewTopic(t.topicName, spec.partitionCount(), spec.replicaCount()).configs(spec.config());
                    });
            return adminClient.createTopics(newTopics.collect(Collectors.toList()));
        } catch (Exception e) {
            throw new RuntimeException("Unable to add missing topics");
        }
    }

    void runStreamApp(Properties config, Topology topology) {
        KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(config));

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
