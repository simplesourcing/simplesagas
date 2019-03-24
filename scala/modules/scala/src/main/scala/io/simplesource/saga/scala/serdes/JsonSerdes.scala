package io.simplesource.saga.scala.serdes

import java.util.stream.Collectors
import java.util.{Optional, UUID}

import io.circe.{Decoder, Encoder}
import io.simplesource.api.CommandId
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.api.{AggregateSerdes, CommandSerdes}
import io.simplesource.kafka.model._
import io.simplesource.saga.model.action.{ActionCommand, ActionId, ActionStatus, SagaAction}
import io.simplesource.saga.model.saga
import io.simplesource.saga.model.saga.{SagaError, SagaId}
import io.simplesource.saga.model.serdes.{ActionSerdes, SagaSerdes}
import org.apache.kafka.common.serialization.Serde

object JsonSerdes {
  import ProductCodecs._
  import JavaCodecs._

  def aggregateSerdes[K: Encoder: Decoder, C: Encoder: Decoder, E: Encoder: Decoder, A: Encoder: Decoder]
    : AggregateSerdes[K, C, E, A] =
    new AggregateSerdes[K, C, E, A] {

      val aks = serdeFromCodecs[K]

      val crs =
        productCodecs4[UUID, K, Long, C, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.commandId().id, v.aggregateKey(), v.readSequence().getSeq, v.command()),
          (id, k, rs, c) => new CommandRequest(CommandId.of(id), k, Sequence.position(rs), c)
        ).asSerde

      val vwss =
        productCodecs2[E, Long, ValueWithSequence[E]]("value", "sequence")(
          v => (v.value(), v.sequence().getSeq),
          (v, s) => new ValueWithSequence(v, Sequence.position(s))
        ).asSerde

      val au = ResultEncoders.au[A]

      val aus = au.asSerde
      val cr  = ResultEncoders.cr[K]

      val cid = mappedCodec[UUID, CommandId](_.id, CommandId.of).asSerde

      override def aggregateKey(): Serde[K]                         = aks
      override def commandRequest(): Serde[CommandRequest[K, C]]    = crs
      override def commandId(): Serde[CommandId]                    = cid
      override def valueWithSequence(): Serde[ValueWithSequence[E]] = vwss
      override def aggregateUpdate(): Serde[AggregateUpdate[A]]     = aus
      override def commandResponse(): Serde[CommandResponse[K]]     = cr
    }

  def commandSerdes[K: Encoder: Decoder, C: Encoder: Decoder]: CommandSerdes[K, C] =
    new CommandSerdes[K, C] {

      val aks = serdeFromCodecs[K]

      val crs: Serde[CommandRequest[K, C]] =
        productCodecs4[K, C, Long, UUID, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.aggregateKey(), v.command(), v.readSequence().getSeq, v.commandId().id),
          (k, v, rs, id) => new CommandRequest(CommandId.of(id), k, Sequence.position(rs), v)
        ).asSerde

      val cid = mappedCodec[UUID, CommandId](_.id, CommandId.of).asSerde
      val cr  = ResultEncoders.cr[K]

      override def aggregateKey(): Serde[K]                      = aks
      override def commandRequest(): Serde[CommandRequest[K, C]] = crs
      override def commandId(): Serde[CommandId]                 = cid
      override def commandResponse(): Serde[CommandResponse[K]]  = cr
    }

  def actionSerdes[A: Encoder: Decoder]: ActionSerdes[A] = new ActionSerdes[A] {
    import io.simplesource.saga.model.messages._

    val u = serdeFromCodecs[UUID]
    val req = productCodecs5[UUID, UUID, UUID, A, String, ActionRequest[A]]("sagaId",
                                                                            "actionId",
                                                                            "commandId",
                                                                            "command",
                                                                            "actionType")(
      v => (v.sagaId.id, v.actionId.id, v.actionCommand.commandId.id, v.actionCommand.command, v.actionType),
      (sId, aId, cId, c, at) =>
        ActionRequest
          .builder[A]()
          .sagaId(SagaId.of(sId))
          .actionId(ActionId.of(aId))
          .actionCommand(new ActionCommand[A](CommandId.of(cId), c))
          .actionType(at)
          .build()
    ).asSerde

    import ResultEncoders._
    def resp: Serde[ActionResponse] = {
      productCodecs4[UUID, UUID, UUID, Result[SagaError, Boolean], ActionResponse]("sagaId",
                                                                                   "actionId",
                                                                                   "commandId",
                                                                                   "sequenceResult")(
        x => (x.sagaId.id, x.actionId.id, x.commandId.id, x.result.map(_ => true)),
        (sagaId, actionId, commandId, result) =>
          new ActionResponse(SagaId.of(sagaId),
                             ActionId.of(actionId),
                             CommandId.of(commandId),
                             result.map(_ => true))
      ).asSerde
    }

    val sid = mappedCodec[UUID, SagaId](_.id, SagaId.of).asSerde
    val aid = mappedCodec[UUID, ActionId](_.id, ActionId.of).asSerde
    val cid = mappedCodec[UUID, CommandId](_.id, CommandId.of).asSerde

    override def commandId(): Serde[CommandId]      = cid
    override def sagaId(): Serde[SagaId]            = sid
    override def actionId(): Serde[ActionId]        = aid
    override def request(): Serde[ActionRequest[A]] = req
    override def response(): Serde[ActionResponse]  = resp
  }

  def sagaSerdes[A: Encoder: Decoder]: SagaSerdes[A] = new SagaSerdes[A] {
    import io.simplesource.saga.model.messages._
    import io.simplesource.saga.model.saga._
    import ResultEncoders._

    import ProductCodecs._
    implicit val (sidEnd, sidDec) = mappedCodec[UUID, SagaId](_.id, SagaId.of)
    private val sid               = (sidEnd, sidDec).asSerde

    implicit val (aidEnd, aidDec) = mappedCodec[UUID, ActionId](_.id, ActionId.of)

    implicit val (acEnc, acDec) =
      productCodecs2[UUID, A, ActionCommand[A]]("commandId", "command")(
        x => (x.commandId.id, x.command),
        (cid, c) => new ActionCommand[A](CommandId.of(cid), c))

    Set(1).map(identity)

    implicit val (saEnc, saDec) = productCodecs7[UUID,
                                                 String,
                                                 ActionCommand[A],
                                                 Optional[ActionCommand[A]],
                                                 java.util.Set[ActionId],
                                                 String,
                                                 java.util.List[SagaError],
                                                 SagaAction[A]]("actionId",
                                                                "actionType",
                                                                "command",
                                                                "undoCommand",
                                                                "dependencies",
                                                                "status",
                                                                "error")(
      x =>
        (x.actionId.id, x.actionType, x.command, x.undoCommand, x.dependencies, x.status.toString, x.error),
      (aid, at, c, uc, d, s, e) =>
        new SagaAction[A](ActionId.of(aid), at, c, uc, d, ActionStatus.valueOf(s), e)
    )

    implicit val (sagaEnc, sagaDec) =
      productCodecs4[SagaId, java.util.Map[ActionId, SagaAction[A]], String, Sequence, Saga[A]](
        "sagaId",
        "actions",
        "status",
        "sequence"
      )(x => (x.sagaId, x.actions, x.status.toString, x.sequence),
        (sid, acts, st, seq) => Saga.of[A](sid, acts, SagaStatus.valueOf(st), seq))

    private val sagaSerde = (sagaEnc, sagaDec).asSerde

    private val sagaRequestSerde =
      productCodecs2[SagaId, Saga[A], SagaRequest[A]]("sagaId", "initialState")(
        x => (x.sagaId, x.initialState),
        (id, init) => new SagaRequest[A](id, init)
      ).asSerde

    import ResultEncoders._
    private val sagaResponseSerde =
      productCodecs2[SagaId, Result[SagaError, Sequence], SagaResponse]("sagaId", "initialState")(
        x => (x.sagaId, x.result),
        (id, init) => new SagaResponse(id, init)
      ).asSerde

    implicit val (initialEnc, initialDec) =
      mappedCodec[Saga[A], SagaStateTransition.SetInitialState[A]](
        _.sagaState,
        new SagaStateTransition.SetInitialState[A](_))

    implicit val (ascEnc, ascDec) =
      productCodecs4[SagaId,
                     ActionId,
                     String,
                     java.util.List[SagaError],
                     SagaStateTransition.SagaActionStatusChanged](
        "sagaId",
        "actionId",
        "actionStatus",
        "error"
      )(
        x => (x.sagaId, x.actionId, x.actionStatus.toString, x.actionErrors),
        (sid, aid, st, e) =>
          new SagaStateTransition.SagaActionStatusChanged(sid, aid, ActionStatus.valueOf(st), e)
      )

    implicit val (sscEnc, sscDec) =
      productCodecs3[SagaId, String, java.util.List[SagaError], SagaStateTransition.SagaStatusChanged](
        "sagaId",
        "sagaStatus",
        "errors"
      )(x => (x.sagaId, x.sagaStatus.toString, x.sagaErrors),
        (sid, ss, es) => new SagaStateTransition.SagaStatusChanged(sid, SagaStatus.valueOf(ss), es))

    implicit val (tlEnc, tlDec) =
      mappedCodec[java.util.List[SagaStateTransition.SagaActionStatusChanged],
                  SagaStateTransition.TransitionList](_.actions,
                                                      x => new SagaStateTransition.TransitionList(x))

    implicit val stateTransitionSerde =
      productCodecs4[
        Option[SagaStateTransition.SetInitialState[A]],
        Option[SagaStateTransition.SagaActionStatusChanged],
        Option[SagaStateTransition.SagaStatusChanged],
        Option[SagaStateTransition.TransitionList],
        SagaStateTransition
      ]("initial", "actionStatus", "sagaStatus", "transitionList")(
        {
          case x: SagaStateTransition.SetInitialState[A] =>
            (Some(x), None, None, None)
          case x: SagaStateTransition.SagaActionStatusChanged =>
            (None, Some(x), None, None)
          case x: SagaStateTransition.SagaStatusChanged =>
            (None, None, Some(x), None)
          case x: SagaStateTransition.TransitionList =>
            (None, None, None, Some(x))
        },
        (is, as, ss, tl) =>
          (is, as, ss, tl) match {
            case (Some(x), _, _, _) => x
            case (_, Some(x), _, _) => x
            case (_, _, Some(x), _) => x
            case (_, _, _, Some(x)) => x
            case _ =>
              throw new Exception("Error in SagaStateTransition deserialization")
        }
      ).asSerde

    override def sagaId(): Serde[SagaId]                  = sid
    override def request(): Serde[SagaRequest[A]]         = sagaRequestSerde
    override def response(): Serde[SagaResponse]          = sagaResponseSerde
    override def state(): Serde[saga.Saga[A]]             = sagaSerde
    override def transition(): Serde[SagaStateTransition] = stateTransitionSerde
  }
}
