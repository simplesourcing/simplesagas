package io.simplesource.saga.scala.serdes
import java.util
import java.util.Optional

import io.circe.{Decoder, Encoder}
import io.simplesource.data.{NonEmptyList, Sequence}
import scala.collection.JavaConverters._

object JavaCodecs {
  implicit def optionalEncoder[A: Encoder]: Encoder[Optional[A]] =
    implicitly[Encoder[Option[A]]].contramap[Optional[A]](optionalA =>
      optionalA.map[Option[A]](a => Option(a)).orElse(None))
  implicit def optionalDecoder[A: Decoder]: Decoder[Optional[A]] =
    implicitly[Decoder[Option[A]]]
      .map(_.fold[Optional[A]](Optional.empty())(a => Optional.of(a)))

  implicit def nelEncoder[A: Encoder]: Encoder[NonEmptyList[A]] =
    implicitly[Encoder[List[A]]]
      .contramap[NonEmptyList[A]](nel => nel.toList.asScala.toList)
  implicit def neld[A: Decoder]: Decoder[NonEmptyList[A]] =
    implicitly[Decoder[List[A]]]
      .map(l => NonEmptyList.fromList(l.asJava).get())

  implicit def seqE(implicit enc: Encoder[Long]): Encoder[Sequence] =
    enc.contramap(_.getSeq)
  implicit def seqD(implicit enc: Decoder[Long]): Decoder[Sequence] =
    enc.map(Sequence.position)

  implicit def mapDecoder[A: Decoder, B: Decoder]
    : Decoder[java.util.Map[A, B]] =
    implicitly[Decoder[List[(A, B)]]].map(m => {
      val l: util.List[(A, B)] = m.asJava
      val map = new util.HashMap[A, B]()
      l.forEach(x => map.put(x._1, x._2))
      map
    })

  implicit def mapEncoder[A: Encoder, B: Encoder]
    : Encoder[java.util.Map[A, B]] =
    implicitly[Encoder[List[(A, B)]]].contramap(_.asScala.toList)

  implicit def setDecoder[A: Decoder]: Decoder[java.util.Set[A]] =
    implicitly[Decoder[Set[A]]].map(_.asJava)

  implicit def setEncoder[A: Encoder]: Encoder[java.util.Set[A]] =
    implicitly[Encoder[Set[A]]].contramap(_.asScala.toSet)

  implicit def listDecoder[A: Decoder]: Decoder[java.util.List[A]] =
    implicitly[Decoder[List[A]]].map(_.asJava)

  implicit def listEncoder[A: Encoder]: Encoder[java.util.List[A]] =
    implicitly[Encoder[List[A]]].contramap(_.asScala.toList)
}
