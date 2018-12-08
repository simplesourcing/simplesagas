package shared.serdes

import io.circe.{Decoder, Encoder}
import io.simplesource.kafka.serialization.util.GenericSerde
import org.slf4j.LoggerFactory
import org.apache.kafka.common.serialization.{Serde, Serdes => KafkaSerdes}

object JsonSerdeUtils {
  private val logger = LoggerFactory.getLogger("JsonSerdeUtils")

  implicit class CodecPairOps[B](codec: (Encoder[B], Decoder[B])) {
    def asSerde: Serde[B] = {
      implicit val encoder: Encoder[B] = codec._1
      implicit val decoder: Decoder[B] = codec._2
      serdeFromCodecs
    }
  }

  def serdeFromCodecs[B](implicit e: Encoder[B], d: Decoder[B]): Serde[B] = {
    import io.circe.parser._
    import io.circe.syntax._
    def toB: java.util.function.Function[String, B] =
      x =>
        parse(x)
          .flatMap(j => j.as[B])
          .fold(err => {
            logger.error(s"Error on:\n$x \n$err")
            throw err
          }, identity)
    def fromB: java.util.function.Function[B, String] = _.asJson.noSpaces

    GenericSerde.of[B, String](KafkaSerdes.String(), fromB, toB)
  }

  def productCodecs2[A0: Encoder: Decoder, A1: Encoder: Decoder, B](n0: String, n1: String)(
    b2p: B => (A0, A1),
    p2b: (A0, A1) => B): (Encoder[B], Decoder[B]) = {
    val encoder: Encoder[B] = Encoder.forProduct2(n0, n1)(b2p)
    val decoder: Decoder[B] =
      Decoder.forProduct2(n0, n1)((a0: A0, a1: A1) => p2b(a0, a1))

    (encoder, decoder)
  }

  def productCodecs3[A0: Encoder: Decoder, A1: Encoder: Decoder, A2: Encoder: Decoder, B](
                                                                                                   n0: String,
                                                                                                   n1: String,
                                                                                                   n2: String)(b2p: B => (A0, A1, A2), p2b: (A0, A1, A2) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct3(n0, n1, n2)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct3(n0, n1, n2)((a0: A0, a1: A1, a2: A2) => p2b(a0, a1, a2))

    (encoder, decoder)
  }

  def productCodecs4[A0: Encoder: Decoder,
  A1: Encoder: Decoder,
  A2: Encoder: Decoder,
  A3: Encoder: Decoder,
  B](n0: String, n1: String, n2: String, n3: String)(
    b2p: B => (A0, A1, A2, A3),
    p2b: (A0, A1, A2, A3) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct4(n0, n1, n2, n3)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct4(n0, n1, n2, n3)((a0: A0, a1: A1, a2: A2, a3: A3) => p2b(a0, a1, a2, a3))

    (encoder, decoder)
  }

  def productCodecs5[A0: Encoder: Decoder,
  A1: Encoder: Decoder,
  A2: Encoder: Decoder,
  A3: Encoder: Decoder,
  A4: Encoder: Decoder,
  B](n0: String, n1: String, n2: String, n3: String, n4: String)(
    b2p: B => (A0, A1, A2, A3, A4),
    p2b: (A0, A1, A2, A3, A4) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct5(n0, n1, n2, n3, n4)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct5(n0, n1, n2, n3, n4)((a0: A0, a1: A1, a2: A2, a3: A3, a4: A4) => p2b(a0, a1, a2, a3, a4))

    (encoder, decoder)
  }

}
