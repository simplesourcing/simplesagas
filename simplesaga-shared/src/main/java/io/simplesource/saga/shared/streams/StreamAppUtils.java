package io.simplesource.saga.shared.streams;

import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.saga.shared.topics.TopicCreation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StreamAppUtils {
    @FunctionalInterface
    public interface ShutdownHandler {
        void shutDown();
    }

    public static void createMissingTopics(Properties config, List<TopicCreation> topics) {
        try {
            StreamAppUtils
                    .createMissingTopics(AdminClient.create(config), topics)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to add missing topics", e);
        }
    }

    public static CreateTopicsResult createMissingTopics(AdminClient adminClient, List<TopicCreation> topics){
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
            throw new RuntimeException("Unable to create missing topics");
        }
    }

    public static void runStreamApp(Properties config, Topology topology) {
        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.cleanUp();
        streams.start();

        addShutdownHook(streams::close);
    }

    public static void shutdownExecutorService(ExecutorService executorService) {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    public static void addShutdownHook(ShutdownHandler handler) {
        Runtime.getRuntime().addShutdownHook(new Thread(handler::shutDown));
    }

}
