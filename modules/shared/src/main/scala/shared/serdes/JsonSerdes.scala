package shared.serdes

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.simplesource.api.CommandError
import io.simplesource.data.{NonEmptyList, Result, Sequence}
import io.simplesource.kafka.api.{AggregateSerdes, CommandSerdes}
import io.simplesource.kafka.model._
import io.simplesource.saga.model.saga
import io.simplesource.saga.model.saga.{ActionCommand, SagaError}
import io.simplesource.saga.model.serdes.{ActionSerdes, SagaSerdes}
import model.saga.Saga
import org.apache.kafka.common.serialization.Serde
import org.slf4j.LoggerFactory

import collection.JavaConverters._

object JsonSerdes {
  import JsonSerdeUtils._

  private val logger = LoggerFactory.getLogger("JsonSerdes")

  object ResultParts {

    final case class LWrapper[E](errors: List[E])
    implicit def nele[E: Encoder]: Encoder[NonEmptyList[E]] =
      io.circe.generic.semiauto
        .deriveEncoder[LWrapper[E]]
        .contramapObject(nel => LWrapper(nel.head() :: nel.tail().asScala.toList))
    implicit def neld[E: Decoder]: Decoder[NonEmptyList[E]] =
      io.circe.generic.semiauto
        .deriveDecoder[LWrapper[E]]
        .map(l => NonEmptyList.fromList(l.errors.asJava).get())

    def au[A: Encoder: Decoder] =
      productCodecs2[A, Long, AggregateUpdate[A]]("aggregate", "sequence")(
        v => (v.aggregate(), v.sequence().getSeq),
        (v, s) => new AggregateUpdate(v, Sequence.position(s))
      )

    implicit def seqE(implicit enc: Encoder[Long]): Encoder[Sequence] = enc.contramap(_.getSeq)
    implicit def seqD(implicit enc: Decoder[Long]): Decoder[Sequence] = enc.map(Sequence.position)

    type EitherNel[E, A] =
      Either[NonEmptyList[E], A]

    implicit def rese[E: Encoder, A: Encoder]: Encoder[Result[E, A]] =
      io.circe.generic.semiauto
        .deriveEncoder[EitherNel[E, A]]
        .contramapObject(r => {
          if (r.isSuccess)
            Right[NonEmptyList[E], A](r.getOrElse(throw new Exception("Error. TODO"))) //TODO
          else
            Left[NonEmptyList[E], A](r.failureReasons().get())
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

    def cr: Serde[CommandResponse] = {
      implicit val cee: Encoder[CommandError] =
        implicitly[Encoder[(String, String)]]
          .contramap[CommandError](ce => (ce.getReason.toString, ce.getMessage))
      implicit val ced: Decoder[CommandError] =
        implicitly[Decoder[(String, String)]]
          .map(s => CommandError.of(CommandError.Reason.valueOf(s._1), s._2))

      productCodecs3[UUID, Long, Result[CommandError, Sequence], CommandResponse]("commandId",
                                                                                  "readSequence",
                                                                                  "sequenceResult")(
        x => (x.commandId(), x.readSequence().getSeq, x.sequenceResult()),
        (id, seq, ur) => new CommandResponse(id, Sequence.position(seq), ur))
    }.asSerde

    import io.simplesource.saga.model.messages._
    def ar: Serde[ActionResponse] = {
      implicit val cee: Encoder[SagaError] =
        implicitly[Encoder[(String, String)]]
          .contramap[SagaError](ce => (ce.getReason.toString, ce.getMessage))
      implicit val ced: Decoder[SagaError] =
        implicitly[Decoder[(String, String)]]
          .map(s => SagaError.of(SagaError.Reason.valueOf(s._1), s._2))

      productCodecs4[UUID, UUID, UUID, Result[SagaError, Boolean], ActionResponse](
        "sagaId",
        "actionId",
        "commandId",
        "sequenceResult")(
        x => (x.sagaId, x.actionId, x.commandId, x.result.map(_ => true)),
        (sagaId, actionId, commandId, result) => new ActionResponse(sagaId, actionId, commandId, result.map(_ => true))).asSerde
    }
  }

  def aggregateSerdes[K: Encoder: Decoder, C: Encoder: Decoder, E: Encoder: Decoder, A: Encoder: Decoder]
    : AggregateSerdes[K, C, E, A] =
    new AggregateSerdes[K, C, E, A] {

      val aks = serdeFromCodecs[K]

      val crs =
        productCodecs4[K, C, Long, UUID, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.aggregateKey(), v.command(), v.readSequence().getSeq, v.commandId()),
          (k, v, rs, id) => new CommandRequest(k, v, Sequence.position(rs), id)
        ).asSerde

      val crks = serdeFromCodecs[UUID]

      val vwss =
        productCodecs2[E, Long, ValueWithSequence[E]]("value", "sequence")(
          v => (v.value(), v.sequence().getSeq),
          (v, s) => new ValueWithSequence(v, Sequence.position(s))
        ).asSerde

      val au = ResultParts.au[A]

      val aus = au.asSerde
      val cr  = ResultParts.cr

      override def aggregateKey(): Serde[K]                         = aks
      override def commandRequest(): Serde[CommandRequest[K, C]]    = crs
      override def commandResponseKey(): Serde[UUID]                = crks
      override def valueWithSequence(): Serde[ValueWithSequence[E]] = vwss
      override def aggregateUpdate(): Serde[AggregateUpdate[A]]     = aus
      override def commandResponse(): Serde[CommandResponse]        = cr
    }

  def commandSerdes[K: Encoder: Decoder, C: Encoder: Decoder]: CommandSerdes[K, C] =
    new CommandSerdes[K, C] {

      val aks = serdeFromCodecs[K]

      val crs: Serde[CommandRequest[K, C]] =
        productCodecs4[K, C, Long, UUID, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.aggregateKey(), v.command(), v.readSequence().getSeq, v.commandId()),
          (k, v, rs, id) => new CommandRequest(k, v, Sequence.position(rs), id)
        ).asSerde

      val crks = serdeFromCodecs[UUID]
      val cr   = ResultParts.cr

      override def aggregateKey(): Serde[K]                      = aks
      override def commandRequest(): Serde[CommandRequest[K, C]] = crs
      override def commandResponseKey(): Serde[UUID]             = crks
      override def commandResponse(): Serde[CommandResponse]     = cr
    }

  def actionSerdesScala[A: Encoder: Decoder]: model.serdes.ActionSerdes[A] = new model.serdes.ActionSerdes[A] {
    import io.circe.generic.auto._
    import model.messages._

    override lazy val uuid: Serde[UUID] = serdeFromCodecs[UUID]
    override lazy val request: Serde[ActionRequest[A]] = serdeFromCodecs[ActionRequest[A]]
    override lazy val response: Serde[ActionResponse] = serdeFromCodecs[ActionResponse]
  }

  def actionSerdes[A: Encoder: Decoder]: ActionSerdes[A] = new ActionSerdes[A] {
    import io.simplesource.saga.model.messages._

    val u = serdeFromCodecs[UUID]
    val req = productCodecs5[UUID, UUID, UUID, A, String, ActionRequest[A]]("sagaId", "actionId", "commandId", "command", "actionType")(v =>
      (v.sagaId, v.actionId, v.actionCommand.commandId, v.actionCommand.command, v.actionType),
      (sId, aId, cId, c, at) => new ActionRequest[A](sId, aId, new ActionCommand[A](cId, c), at)).asSerde

    val resp = ResultParts.ar

    override def uuid(): Serde[UUID] = u
    override def request(): Serde[ActionRequest[A]] = req
    override def response(): Serde[ActionResponse] = resp
  }

  def sagaSerdesScala[A: Encoder: Decoder]: model.serdes.SagaSerdes[A] = new model.serdes.SagaSerdes[A] {
    import io.circe.generic.auto._
    import model.messages._
    override lazy val uuid: Serde[UUID]                         = serdeFromCodecs[UUID]
    override lazy val request: Serde[SagaRequest[A]]            = serdeFromCodecs[SagaRequest[A]]
    override lazy val response: Serde[SagaResponse]             = serdeFromCodecs[SagaResponse]
    override lazy val state: Serde[Saga[A]]                     = serdeFromCodecs[Saga[A]]
    override lazy val transition: Serde[SagaStateTransition[A]] = serdeFromCodecs[SagaStateTransition[A]]
  }

  def sagaSerdes[A: Encoder: Decoder]: SagaSerdes[A] = new SagaSerdes[A] {
    import io.simplesource.saga.model.messages._
//    override lazy val uuid: Serde[UUID]                         = serdeFromCodecs[UUID]
//    override lazy val request: Serde[SagaRequest[A]]            = serdeFromCodecs[SagaRequest[A]]
//    override lazy val response: Serde[SagaResponse]             = serdeFromCodecs[SagaResponse]
//    override lazy val state: Serde[Saga[A]]                     = serdeFromCodecs[Saga[A]]
//    override lazy val transition: Serde[SagaStateTransition[A]] = serdeFromCodecs[SagaStateTransition[A]]
    override def uuid(): Serde[UUID] = serdeFromCodecs[UUID]
    override def request(): Serde[SagaRequest[A]] = ???
    override def response(): Serde[SagaResponse] = ???
    override def state(): Serde[saga.Saga[A]] = ???
    override def transition(): Serde[SagaStateTransition[A]] = ???
  }
}
