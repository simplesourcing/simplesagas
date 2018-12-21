//package action.sourcing
//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.kafka.common.serialization.Serde
//import org.apache.kafka.streams.test.ConsumerRecordFactory
//import org.apache.kafka.streams.{StreamsBuilder, TopologyTestDriver}
//import io.simplesource.saga.user.shared.utils.{StreamAppConfig, StreamAppUtils}
//
//object TestUtils {
//  trait TestProducer[K, V] {
//    def pipeInput(k: K, v: V): Unit
//  }
//
//  final case class ContextDriver[A, I, C, K](ctx: SourcingContext[A, I, C, K],
//                                             buildOperations: StreamsBuilder => Unit) {
//    val streamConfig = StreamAppUtils.getConfig(StreamAppConfig("test-action-streams", "127.0.0.1:9092"))
//    val driver: TopologyTestDriver = {
//      val builder = new StreamsBuilder()
//      buildOperations(builder)
//      new TopologyTestDriver(builder.build, streamConfig, 0L)
//    }
//
//    def produce[KR, VR](topicName: String, keySerde: Serde[KR], valueSerde: Serde[VR]): TestProducer[KR, VR] =
//      new TestProducer[KR, VR] {
//        val producer =
//          new ConsumerRecordFactory(topicName: String, keySerde.serializer(), valueSerde.serializer())
//
//        override def pipeInput(k: KR, v: VR): Unit = driver.pipeInput(producer.create(k, v))
//      }
//
//    def readOutput[KR, VR](topicName: String,
//                           keySerde: Serde[KR],
//                           valueSerde: Serde[VR]): ProducerRecord[KR, VR] =
//      driver.readOutput(topicName, keySerde.deserializer(), valueSerde.deserializer())
//  }
//}
