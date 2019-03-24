package io.simplesource.saga.scala.serdes
import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.simplesource.api.{CommandError, CommandId}
import io.simplesource.data.{NonEmptyList, Result, Sequence}
import io.simplesource.kafka.model.{AggregateUpdate, CommandResponse}
import io.simplesource.saga.model.saga.SagaError
import org.apache.kafka.common.serialization.Serde

object ResultEncoders {
  import JavaCodecs._
  import ProductCodecs._

  def au[A: Encoder: Decoder]: (Encoder[AggregateUpdate[A]], Decoder[AggregateUpdate[A]]) =
    productCodecs2[A, Long, AggregateUpdate[A]]("aggregate", "sequence")(
      v => (v.aggregate(), v.sequence().getSeq),
      (v, s) => new AggregateUpdate(v, Sequence.position(s))
    )

  type EitherNel[E, A] =
    Either[NonEmptyList[E], A]

  implicit def rese[E: Encoder, A: Encoder]: Encoder[Result[E, A]] =
    io.circe.generic.semiauto
      .deriveEncoder[EitherNel[E, A]]
      .contramapObject(r => {
        r.fold[EitherNel[E, A]](e => Left[NonEmptyList[E], A](e), a => Right[NonEmptyList[E], A](a))
      })

  implicit def resd[E: Decoder, A: Decoder]: Decoder[Result[E, A]] =
    io.circe.generic.semiauto
      .deriveDecoder[EitherNel[E, A]]
      .map({
        case Right(r) =>
          Result.success[E, A](r)
        case Left(e) =>
          Result.failure[E, A](e)
      })

  def cr[K: Encoder: Decoder]: Serde[CommandResponse[K]] = {
    implicit val cee: Encoder[CommandError] =
      implicitly[Encoder[(String, String)]]
        .contramap[CommandError](ce => (ce.getReason.toString, ce.getMessage))
    implicit val ced: Decoder[CommandError] =
      implicitly[Decoder[(String, String)]]
        .map(s => CommandError.of(CommandError.Reason.valueOf(s._1), s._2))

    productCodecs4[K, UUID, Long, Result[CommandError, Sequence], CommandResponse[K]]("key",
                                                                                      "commandId",
                                                                                      "readSequence",
                                                                                      "sequenceResult")(
      x => (x.aggregateKey(), x.commandId.id, x.readSequence().getSeq, x.sequenceResult()),
      (key, id, seq, ur) => new CommandResponse(CommandId.of(id), key, Sequence.position(seq), ur)
    )
  }.asSerde

  implicit val cee: Encoder[SagaError] =
    implicitly[Encoder[(String, String)]]
      .contramap[SagaError](ce => (ce.getReason.toString, ce.getMessage))
  implicit val ced: Decoder[SagaError] =
    implicitly[Decoder[(String, String)]]
      .map(s => SagaError.of(SagaError.Reason.valueOf(s._1), s._2))
}
