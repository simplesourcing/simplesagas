package io.simplesource.saga.action.sourcing;


import io.simplesource.kafka.model.CommandResponse;
import io.simplesource.saga.action.common.ActionConsumer;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.shared.topics.TopicConfig;
import io.simplesource.saga.shared.topics.TopicConfigBuilder;
import io.simplesource.saga.shared.topics.TopicCreation;
import io.simplesource.saga.shared.topics.TopicTypes;
import io.simplesource.saga.shared.utils.StreamAppConfig;
import io.simplesource.saga.shared.utils.StreamAppUtils;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class SourcingApp<A> {

    private final Logger logger = LoggerFactory.getLogger(SourcingApp.class);

    private final TopicConfig actionTopicConfig;
    private final ActionSerdes<A> actionSerdes;

    private final List<Command<A>> commands = new ArrayList<>();
    private final List<TopicCreation> topicCreations;

    @Value
    private static final class CommandInput<A> {
        final StreamsBuilder builder;
        final KStream<UUID, ActionRequest<A>> actionRequestKStream;
        final KStream<UUID, ActionResponse> actionResponseKStream;
    }

    interface Command<A> {
        void applyCommandInput(CommandInput<A> input);
    }

    public SourcingApp(ActionSerdes<A> actionSerdes, TopicConfigBuilder.BuildSteps topicBuildFn) {
        actionTopicConfig =
                TopicConfigBuilder.buildTopics(TopicTypes.ActionTopic.all, new HashMap<>(), new HashMap<>(), topicBuildFn);
        this.actionSerdes = actionSerdes;
        topicCreations = TopicCreation.allTopics(actionTopicConfig);
    }

    public <K, C> SourcingApp<A> addCommand(CommandSpec<A, K, C> cSpec, TopicConfigBuilder.BuildSteps topicBuildSteps) {
        TopicConfig commandTopicConfig = TopicConfigBuilder.buildTopics(TopicTypes.CommandTopic.all, new HashMap<>(), new HashMap<>(), topicBuildSteps);
        ActionContext<A> actionContext = new ActionContext<>(actionSerdes, actionTopicConfig.namer);

        Command<A> command = input -> {
            KStream<K, CommandResponse> commandResponseKStream = CommandResponseConsumer.commandResponseStream(cSpec.commandSerdes(), commandTopicConfig.namer, input.builder);
            SourcingStream.addSubTopology(actionContext,
                    cSpec,
                    input.actionRequestKStream,
                    input.actionResponseKStream,
                    commandResponseKStream);
        };

        commands.add(command);
        topicCreations.addAll(TopicCreation.allTopics(commandTopicConfig));
        return this;
    }


    public void run(StreamAppConfig appConfig) {
        Properties config = StreamAppConfig.getConfig(appConfig);

        try {
            StreamAppUtils
                    .addMissingTopics(AdminClient.create(config), topicCreations)
                    .all()
                    .get(30L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create all the topics");
        }

        StreamsBuilder builder = new StreamsBuilder();

        KStream<UUID, ActionRequest<A>> actionRequests =
                ActionConsumer.actionRequestStream(actionSerdes, actionTopicConfig.namer, builder);
        KStream<UUID, ActionResponse> actionResponses =
                ActionConsumer.actionResponseStream(actionSerdes, actionTopicConfig.namer, builder);

        CommandInput<A> commandInput = new CommandInput<>(builder, actionRequests, actionResponses);


        for (Command<A> c : commands) {
            c.applyCommandInput(commandInput);
        }

        Topology topology = builder.build();
        logger.info("Topology description {}", topology.describe());
        StreamAppUtils.runStreamApp(config, topology);
    }
}
