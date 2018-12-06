//package io.simplesource.saga.saga.app
//
//import java.util.UUID
//
//import io.simplesource.kafka.spec.WindowSpec
//import org.apache.kafka.common.serialization.{Serde, Serdes}
//import org.apache.kafka.streams.StreamsBuilder
//import org.apache.kafka.streams.kstream._
//import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}
//import shared.streams.syntax._
//
//// TODO: really should find a way to share the code in SimpleSourcing
//final case class DistributorSerdes<V>(uuid: Serde<UUID>, value: Serde<V>)
//
//final case class DistributorContext<V>(topicNameMapTopic: String,
//                                       serdes: DistributorSerdes<V>,
//                                       responseWindowSpec: WindowSpec,
//                                       idMapper: V => UUID)
//
//object ResultDistributor {
//
//  def resultTopicMapStream<V>(ctx: DistributorContext<V>, builder: StreamsBuilder) =
//    builder.stream(ctx.topicNameMapTopic, Consumed.`with`(ctx.serdes.uuid, Serdes.String))
//
//  def distribute<K, V>(ctx: DistributorContext<V>,
//                       resultStream: KStream<K, V>,
//                       topicNameStream: KStream<UUID, String>): Unit = {
//    val serdes          = ctx.serdes
//    val retentionMillis = ctx.responseWindowSpec.retentionInSeconds * 1000L
//
//    val topicNameExtractor: TopicNameExtractor<String, V> = (key: String, _: V, _: RecordContext) =>
//      key.substring(0, key.length - 37)
//
//    val valueJoiner: ValueJoiner<V, String, (V, String)> = (res, name) => (res, name)
//    val keyValueMapper: KeyValueMapper<K, V, UUID>       = (_: K, v: V) => ctx.idMapper.apply(v)
//
//    val joined = resultStream
//      .selectKey<UUID>(keyValueMapper)
//      .join<String, (V, String)>(topicNameStream,
//                                 valueJoiner,
//                                 JoinWindows.of(retentionMillis).until(retentionMillis * 2 + 1),
//                                 Joined.`with`(serdes.uuid, serdes.value, Serdes.String))
//      ._map((uuid: UUID, tuple: (V, String)) => (String.format("%s:%s", tuple._2, uuid.toString), tuple._1))
//    joined.to(topicNameExtractor, Produced.`with`(Serdes.String, serdes.value))
//  }
//}
