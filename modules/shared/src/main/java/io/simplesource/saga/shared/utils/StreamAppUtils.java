package io.simplesource.saga.shared.utils;

import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.saga.shared.topics.TopicCreation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamAppUtils {
    public static final CreateTopicsResult addMissingTopics(AdminClient adminClient, List<TopicCreation> topics){
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

    public static void runStreamApp(Properties config, Topology topology) {
        KafkaStreams streams = new KafkaStreams(topology, new StreamsConfig(config));

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
