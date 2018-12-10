package shared.serdes

import java.util.{Optional, UUID}

import io.circe.{Decoder, Encoder}
import io.simplesource.api.CommandError
import io.simplesource.data.{NonEmptyList, Result, Sequence}
import io.simplesource.kafka.api.{AggregateSerdes, CommandSerdes}
import io.simplesource.kafka.model._
import io.simplesource.saga.model.saga
import io.simplesource.saga.model.saga.{ActionCommand, ActionStatus, SagaError}
import io.simplesource.saga.model.serdes.{ActionSerdes, SagaSerdes}
import org.apache.kafka.common.serialization.Serde
import org.slf4j.LoggerFactory

object JsonSerdes {
  import JsonSerdeUtils._

  private val logger = LoggerFactory.getLogger("JsonSerdes")

  object ResultParts {

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

    implicit val cee: Encoder[SagaError] =
      implicitly[Encoder[(String, String)]]
        .contramap[SagaError](ce => (ce.getReason.toString, ce.getMessage))
    implicit val ced: Decoder[SagaError] =
      implicitly[Decoder[(String, String)]]
        .map(s => SagaError.of(SagaError.Reason.valueOf(s._1), s._2))

    def ar: Serde[ActionResponse] = {
      productCodecs4[UUID, UUID, UUID, Result[SagaError, Boolean], ActionResponse]("sagaId",
                                                                                   "actionId",
                                                                                   "commandId",
                                                                                   "sequenceResult")(
        x => (x.sagaId, x.actionId, x.commandId, x.result.map(_ => true)),
        (sagaId, actionId, commandId, result) =>
          new ActionResponse(sagaId, actionId, commandId, result.map(_ => true))
      ).asSerde
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

  def actionSerdesScala[A: Encoder: Decoder]: model.serdes.ActionSerdes[A] =
    new model.serdes.ActionSerdes[A] {
      import io.circe.generic.auto._
      import model.messages._

      override lazy val uuid: Serde[UUID]                = serdeFromCodecs[UUID]
      override lazy val request: Serde[ActionRequest[A]] = serdeFromCodecs[ActionRequest[A]]
      override lazy val response: Serde[ActionResponse]  = serdeFromCodecs[ActionResponse]
    }

  object ActionParts {

  }

  def actionSerdes[A: Encoder: Decoder]: ActionSerdes[A] = new ActionSerdes[A] {
    import io.simplesource.saga.model.messages._

    val u = serdeFromCodecs[UUID]
    val req = productCodecs5[UUID, UUID, UUID, A, String, ActionRequest[A]]("sagaId",
                                                                            "actionId",
                                                                            "commandId",
                                                                            "command",
                                                                            "actionType")(
      v => (v.sagaId, v.actionId, v.actionCommand.commandId, v.actionCommand.command, v.actionType),
      (sId, aId, cId, c, at) => new ActionRequest[A](sId, aId, new ActionCommand[A](cId, c), at)
    ).asSerde

    val resp = ResultParts.ar

    override def uuid(): Serde[UUID]                = u
    override def request(): Serde[ActionRequest[A]] = req
    override def response(): Serde[ActionResponse]  = resp
  }

  def sagaSerdesScala[A: Encoder: Decoder]: model.serdes.SagaSerdes[A] = new model.serdes.SagaSerdes[A] {
    import io.circe.generic.auto._
    import model.messages._
    import model.saga._
    override lazy val uuid: Serde[UUID]                         = serdeFromCodecs[UUID]
    override lazy val request: Serde[SagaRequest[A]]            = serdeFromCodecs[SagaRequest[A]]
    override lazy val response: Serde[SagaResponse]             = serdeFromCodecs[SagaResponse]
    override lazy val state: Serde[Saga[A]]                     = serdeFromCodecs[Saga[A]]
    override lazy val transition: Serde[SagaStateTransition[A]] = serdeFromCodecs[SagaStateTransition[A]]
  }

  def sagaSerdes[A: Encoder: Decoder]: SagaSerdes[A] = new SagaSerdes[A] {
    import io.simplesource.saga.model.messages._
    import io.simplesource.saga.model.saga._
    import ResultParts.ced
    import ResultParts.cee

    val u = serdeFromCodecs[UUID]
    import JsonSerdeUtils._

    implicit val (acEnc, acDec) = productCodecs2[UUID, A, ActionCommand[A]]("commandId", "command")(
      x => (x.commandId, x.command),
      (cid, c) => new ActionCommand[A](cid, c))

    implicit val (saEnc, saDec) = productCodecs7[UUID,
      String,
      ActionCommand[A],
      Optional[ActionCommand[A]],
      java.util.Set[UUID],
      String,
      Optional[SagaError],
      SagaAction[A]]("actionId",
      "actionType",
      "command",
      "undoCommand",
      "dependencies",
      "status",
      "error")(
      x => (x.actionId, x.actionType, x.command, x.undoCommand, x.dependencies, x.status.toString, x.error),
      (aid, at, c, uc, d, s, e) => new SagaAction[A](aid, at, c, uc, d, ActionStatus.valueOf(s), e)
    )

    implicit val (sagaEnc, sagaDec) =
      productCodecs4[UUID, java.util.Map[UUID, SagaAction[A]], String, Sequence, Saga[A]](
        "sagaId",
        "actions",
        "status",
        "sequence"
      )(x => (x.sagaId, x.actions, x.status.toString, x.sequence),
        (sid, acts, st, seq) => new Saga[A](sid, acts, SagaStatus.valueOf(st), seq))

    private val sagaSerde = (sagaEnc, sagaDec).asSerde

    private val sagaRequestSerde = productCodecs2[UUID, Saga[A], SagaRequest[A]]("sagaId", "initialState")(
      x => (x.sagaId, x.initialState), (id, init) => new SagaRequest[A](id, init)
    ).asSerde

    import ResultParts._
    private val sagaResponseSerde = productCodecs2[UUID, Result[SagaError, Sequence], SagaResponse]("sagaId", "initialState")(
      x => (x.sagaId, x.result), (id, init) => new SagaResponse(id, init)
    ).asSerde

    implicit val (initialEnc, initialDec) =
      mappedCodec[Saga[A], SagaStateTransition.SetInitialState[A]](_.sagaState,
        new SagaStateTransition.SetInitialState[A](_))

    implicit val (ascEnc, ascDec) =
      productCodecs4[UUID, UUID, String, Optional[SagaError], SagaStateTransition.SagaActionStatusChanged](
        "sagaId",
        "actionId",
        "actionStatus",
        "error"
      )(x => (x.sagaId, x.actionId, x.actionStatus.toString, x.actionError),
        (sid, aid, st, e) => new SagaStateTransition.SagaActionStatusChanged(sid, aid, ActionStatus.valueOf(st), e))

    implicit val (sscEnc, sscDec) =
      productCodecs3[UUID, String, Optional[NonEmptyList[SagaError]], SagaStateTransition.SagaStatusChanged](
        "sagaId",
        "sagaStatus",
        "errors"
      )(x => (x.sagaId, x.sagaStatus.toString, x.actionErrors), (sid, ss, es) => new SagaStateTransition.SagaStatusChanged(sid, SagaStatus.valueOf(ss), es))

    implicit val (tlEnc, tlDec) =
      mappedCodec[
        java.util.List[SagaStateTransition.SagaActionStatusChanged],
        SagaStateTransition.TransitionList](_.actionError, x => new SagaStateTransition.TransitionList(x))

    implicit val (stEnc, stDec) = {
      productCodecs4[
        Option[SagaStateTransition.SetInitialState[A]],
        Option[SagaStateTransition.SagaActionStatusChanged],
        Option[SagaStateTransition.SagaStatusChanged],
        Option[SagaStateTransition.TransitionList],
        SagaStateTransition[A]
      ]("initial", "actionStatus", "sagaStatus", "transitionList")(x =>
        x match {
        case x: SagaStateTransition.SagaActionStatusChanged => (None, Some(x), None, None)
        case _ => throw new Exception()
      }, (is, as, ss, tl) => (is, as, ss, tl) match {
        case (Some(x), _, _, _) => x
      })
    }

    //    override lazy val uuid: Serde[UUID]                         = serdeFromCodecs[UUID]
//    override lazy val request: Serde[SagaRequest[A]]            = serdeFromCodecs[SagaRequest[A]]
//    override lazy val response: Serde[SagaResponse]             = serdeFromCodecs[SagaResponse]
//    override lazy val state: Serde[Saga[A]]                     = serdeFromCodecs[Saga[A]]
//    override lazy val transition: Serde[SagaStateTransition[A]] = serdeFromCodecs[SagaStateTransition[A]]

    override def uuid(): Serde[UUID]                         = serdeFromCodecs[UUID]
    override def request(): Serde[SagaRequest[A]]            = sagaRequestSerde
    override def response(): Serde[SagaResponse]             = sagaResponseSerde
    override def state(): Serde[saga.Saga[A]]                = sagaSerde
    override def transition(): Serde[SagaStateTransition] = ???
  }
}
