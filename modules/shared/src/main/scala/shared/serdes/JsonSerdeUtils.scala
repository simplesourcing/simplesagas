package shared.serdes

import java.util
import java.util.Optional

import io.circe.{Decoder, Encoder}
import io.simplesource.data.{NonEmptyList, Sequence}
import io.simplesource.kafka.serialization.util.GenericSerde
import org.slf4j.LoggerFactory
import org.apache.kafka.common.serialization.{Serde, Serdes => KafkaSerdes}

import scala.collection.JavaConverters._

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
      Decoder.forProduct5(n0, n1, n2, n3, n4)((a0: A0, a1: A1, a2: A2, a3: A3, a4: A4) =>
        p2b(a0, a1, a2, a3, a4))

    (encoder, decoder)
  }

  def productCodecs7[A0: Encoder: Decoder,
                     A1: Encoder: Decoder,
                     A2: Encoder: Decoder,
                     A3: Encoder: Decoder,
                     A4: Encoder: Decoder,
                     A5: Encoder: Decoder,
                     A6: Encoder: Decoder,
                     B](n0: String, n1: String, n2: String, n3: String, n4: String, n5: String, n6: String)(
      b2p: B => (A0, A1, A2, A3, A4, A5, A6),
      p2b: (A0, A1, A2, A3, A4, A5, A6) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct7(n0, n1, n2, n3, n4, n5, n6)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct7(n0, n1, n2, n3, n4, n5, n6)(
        (a0: A0, a1: A1, a2: A2, a3: A3, a4: A4, a5: A5, a6: A6) => p2b(a0, a1, a2, a3, a4, a5, a6))

    (encoder, decoder)
  }

  implicit def optionalEncoder[A: Encoder]: Encoder[Optional[A]] =
    implicitly[Encoder[Option[A]]].contramap[Optional[A]](optionalA =>
      optionalA.map[Option[A]](a => Option(a)).orElse(None))
  implicit def optionalDecoder[A: Decoder]: Decoder[Optional[A]] =
    implicitly[Decoder[Option[A]]].map(_.fold[Optional[A]](Optional.empty())(a => Optional.of(a)))

  implicit def nelEncoder[A: Encoder]: Encoder[NonEmptyList[A]] =
    implicitly[Encoder[List[A]]]
      .contramap[NonEmptyList[A]](nel => nel.toList.asScala.toList)
  implicit def neld[A: Decoder]: Decoder[NonEmptyList[A]] =
    implicitly[Decoder[List[A]]]
      .map(l => NonEmptyList.fromList(l.asJava).get())

  implicit def seqE(implicit enc: Encoder[Long]): Encoder[Sequence] = enc.contramap(_.getSeq)
  implicit def seqD(implicit enc: Decoder[Long]): Decoder[Sequence] = enc.map(Sequence.position)

  implicit def mapDecoder[A: Decoder, B: Decoder]: Decoder[java.util.Map[A, B]] =
    implicitly[Decoder[List[(A, B)]]].map(m => {
      val l: util.List[(A, B)] = m.asJava
      val map                  = new util.HashMap[A, B]()
      l.forEach(x => map.put(x._1, x._2))
      map
    })

  implicit def mapEncoder[A: Encoder, B: Encoder]: Encoder[java.util.Map[A, B]] =
    implicitly[Encoder[List[(A, B)]]].contramap(_.asScala.toList)

  implicit def setDecoder[A: Decoder]: Decoder[java.util.Set[A]] =
    implicitly[Decoder[Set[A]]].map(_.asJava)

  implicit def setEncoder[A: Encoder]: Encoder[java.util.Set[A]] =
    implicitly[Encoder[Set[A]]].contramap(_.asScala.toSet)
}
