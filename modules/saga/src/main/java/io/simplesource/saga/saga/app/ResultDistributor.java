package io.simplesource.saga.saga.app;

import io.simplesource.kafka.internal.util.Tuple2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.util.UUID;


//// TODO: really should find a way to share the code in SimpleSourcing
public class ResultDistributor {

  public static <V> KStream<UUID, String> resultTopicMapStream(DistributorContext<V> ctx, StreamsBuilder builder) {
      return builder.stream(ctx.topicNameMapTopic, Consumed.with(ctx.serdes.uuid, Serdes.String()));
  }

  public static <K, V> void distribute(DistributorContext<V> ctx,
                       KStream<K, V> resultStream,
                       KStream<UUID, String> topicNameStream) {
      DistributorSerdes<V> serdes          = ctx.serdes;
    long retentionMillis = ctx.responseWindowSpec.retentionInSeconds() * 1000L;

      TopicNameExtractor<String, V> topicNameExtractor = (key, v, c) ->
        key.substring(0, key.length() - 37);

      KStream<String, V> joined = resultStream
              .selectKey((k, v) -> ctx.idMapper.apply(v))
              .join(topicNameStream,
                      Tuple2::of,
                      JoinWindows.of(retentionMillis).until(retentionMillis * 2 + 1),
                      Joined.with(serdes.uuid, serdes.value, Serdes.String()))
              .map((uuid, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), uuid.toString()), tuple.v1()));
    joined.to(topicNameExtractor, Produced.with(Serdes.String(), serdes.value));
  }
}
