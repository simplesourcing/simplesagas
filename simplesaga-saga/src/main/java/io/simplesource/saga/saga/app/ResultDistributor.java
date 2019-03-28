package io.simplesource.saga.saga.app;

import io.simplesource.kafka.internal.util.Tuple2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TopicNameExtractor;

import java.time.Duration;
import java.util.UUID;


//// TODO: really should find a way to share the code in SimpleSourcing
public class ResultDistributor {

  public static <K, V> KStream<K, String> resultTopicMapStream(DistributorContext<K, V> ctx, StreamsBuilder builder) {
      return builder.stream(ctx.topicNameMapTopic, Consumed.with(ctx.serdes.key, Serdes.String()));
  }

  public static <K, V> void distribute(DistributorContext<K, V> ctx,
                       KStream<K, V> resultStream,
                       KStream<K, String> topicNameStream) {
      DistributorSerdes<K, V> serdes          = ctx.serdes;
    Duration retention = Duration.ofSeconds(ctx.responseWindowSpec.retentionInSeconds());

      TopicNameExtractor<String, V> topicNameExtractor = (key, v, c) ->
        key.substring(0, key.length() - 37);

      KStream<String, V> joined = resultStream
              .selectKey((k, v) -> ctx.idMapper.apply(v))
              .join(topicNameStream,
                      Tuple2::of,
                      JoinWindows.of(retention).until(retention.toMillis() * 2 + 1),
                      Joined.with(serdes.key, serdes.value, Serdes.String()))
              .map((key, tuple) -> KeyValue.pair(String.format("%s:%s", tuple.v2(), ctx.keyToUuid.apply(key).toString()), tuple.v1()));
    joined.to(topicNameExtractor, Produced.with(Serdes.String(), serdes.value));
  }
}
