package action.sourcing
import java.util.UUID

import action.common.{ActionProducer, IdempotentStream, Utils}
import io.simplesource.api.CommandError
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.model.{CommandRequest, CommandResponse}
import model.messages.{ActionRequest, ActionResponse}
import model.saga.SagaError
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.LoggerFactory
import shared.streams.syntax._

object SourcingStream {
  private val logger = LoggerFactory.getLogger("SourcingStream")

  def logValues[K, V](prefix: String): ForeachAction[K, V] = Utils.logValues[K, V](logger, prefix)

  def addSubTopology[A, I, K, C](ctx: SourcingContext[A, I, K, C],
                                 actionRequest: KStream[UUID, ActionRequest[A]],
                                 actionResponse: KStream[UUID, ActionResponse],
                                 commandResponseByAggregate: KStream[K, CommandResponse]): Unit = {
    val commandResponseByCommandId = commandResponseByAggregate.selectKey[UUID]((_, v) => v.commandId())

    // join the action request with corresponding prior command responses
    val idempotentAction =
      IdempotentStream.getActionRequestsWithResponse(ctx.actionSpec,
                                                     actionRequest,
                                                     actionResponse,
                                                     ctx.commandSpec.actionType)

    // get new command requests
    val (requestErrorResponses, commandRequests) =
      handleActionRequest(ctx, idempotentAction.unprocessedRequests, commandResponseByAggregate)

    // handle incoming command responses
    val newActionResponses = handleCommandResponse(ctx, actionRequest, commandResponseByCommandId)

    // publish to output topics
    CommandProducer.commandRequest(ctx.commandSpec, commandRequests)
    ActionProducer.actionResponse(ctx.actionSpec,
                                  idempotentAction.priorResponses,
                                  newActionResponses,
                                  requestErrorResponses)
  }

  def handleActionRequest[A, I, K, C](ctx: SourcingContext[A, I, K, C],
                                      actionRequests: KStream[UUID, ActionRequest[A]],
                                      commandResponseByAggregate: KStream[K, CommandResponse])
    : (KStream[UUID, ActionResponse], KStream[K, CommandRequest[K, C]]) = {

    def getAggregateSequence(cResp: CommandResponse): Sequence =
      cResp.sequenceResult().getOrElse(cResp.readSequence())

    val reqsWithDecoded: KStream[UUID, (ActionRequest[A], Either[Throwable, I])] =
      actionRequests._mapValues(ar => (ar, ctx.commandSpec.decode(ar.actionCommand.command)))

    val errorActionRequests = reqsWithDecoded
      ._collect {
        case (ard, Left(e)) =>
          ActionResponse(sagaId = ard.sagaId,
                         actionId = ard.actionId,
                         commandId = ard.actionCommand.commandId,
                         Left(SagaError.of(e.getMessage)))
      }

    val allGood = reqsWithDecoded._collect { case (ar, Right(i)) => (ar, i) }

    // Sort incoming request by the aggregate key
    val requestByAggregateKey: KStream[K, ActionRequest[A]] = allGood
      ._map((_, aReq: (ActionRequest[A], I)) => (ctx.commandSpec.keyMapper(aReq._2), aReq._1))
      .peek(logValues[K, ActionRequest[A]]("requestByAggregateKey"))

    // Get the most recent command response for the aggregate
    val lastCommandByAggregate: KTable[K, CommandResponse] = {
      val materializer: Materialized[K, CommandResponse, KeyValueStore[Bytes, Array[Byte]]] =
        Materialized
          .as[K, CommandResponse, KeyValueStore[Bytes, Array[Byte]]](
            "last_command_by_aggregate_" + ctx.commandSpec.aggregateName)
          .withKeySerde(ctx.cSerdes.aggregateKey)
          .withValueSerde(ctx.cSerdes.response)

      commandResponseByAggregate
        .groupByKey()
        .reduce((cr1: CommandResponse, cr2: CommandResponse) =>
                  if (getAggregateSequence(cr2).isGreaterThan(getAggregateSequence(cr1))) cr2
                  else cr1,
                materializer)
    }

    // Get the latest sequence number and turn action request into a command request
    val commandRequestByAggregate: KStream[K, CommandRequest[K, C]] = {
      val valueJoiner: ValueJoiner[ActionRequest[A], CommandResponse, CommandRequest[K, C]] =
        (aReq: ActionRequest[A], cResp: CommandResponse) => {
          val sequence =
            if (cResp == null) Sequence.first() else getAggregateSequence(cResp)
          // we can do this safely as we have (unfortunately) already done this, and succeeded first time
          val intermediate = ctx.commandSpec.decode(aReq.actionCommand.command).right.get
          new CommandRequest[K, C](ctx.commandSpec.keyMapper(intermediate),
                                   ctx.commandSpec.commandMapper(intermediate),
                                   sequence,
                                   aReq.actionCommand.commandId)
        }

      requestByAggregateKey.leftJoin[CommandResponse, CommandRequest[K, C]](
        lastCommandByAggregate,
        valueJoiner,
        Joined.`with`[K, ActionRequest[A], CommandResponse](ctx.cSerdes.aggregateKey,
                                                            ctx.aSerdes.request,
                                                            ctx.cSerdes.response)
      )
    }.peek(logValues[K, CommandRequest[K, C]]("commandRequestByAggregate"))

    (errorActionRequests, commandRequestByAggregate)
  }

  def handleCommandResponse[A, I, K, C](
      ctx: SourcingContext[A, I, K, C],
      actionRequests: KStream[UUID, ActionRequest[A]],
      responseByCommandId: KStream[UUID, CommandResponse]): KStream[UUID, ActionResponse] = {
    // find the response for the request
    val actionRequestWithResponse: KStream[UUID, (ActionRequest[A], CommandResponse)] = {
      val valueJoiner: ValueJoiner[ActionRequest[A], CommandResponse, (ActionRequest[A], CommandResponse)] =
        (aReq: ActionRequest[A], cResp: CommandResponse) => (aReq, cResp)

      // join command response to action request by the command / action ID
      // TODO: timeouts - will be easy to do timeouts with a left join once https://issues.apache.org/jira/browse/KAFKA-6556 has been released
      val timeOutMillis = ctx.commandSpec.timeOutMillis
      actionRequests
        .selectKey[UUID]((_: UUID, aReq: ActionRequest[A]) => aReq.actionCommand.commandId)
        .join[CommandResponse, (ActionRequest[A], CommandResponse)](
          responseByCommandId,
          valueJoiner,
          JoinWindows.of(timeOutMillis).until(timeOutMillis * 2 + 1),
          Joined.`with`(ctx.aSerdes.uuid, ctx.aSerdes.request, ctx.cSerdes.response)
        )
        .peek(logValues[UUID, (ActionRequest[A], CommandResponse)]("handleCommandResponse"))

    }

    // turn the pair into an ActionResponse
    getActionResponse(ctx, actionRequestWithResponse)
  }

  def getActionResponse[A, I, K, C](
      ctx: SourcingContext[A, I, K, C],
      actionRequestWithResponse: KStream[UUID, (ActionRequest[A], CommandResponse)])
    : KStream[UUID, ActionResponse] = {
    val actionResponse: KStream[UUID, ActionResponse] = actionRequestWithResponse
      ._mapValues[ActionResponse] {
        case (aReq, cResp) =>
          val sequenceResult: Result[CommandError, Sequence] =
            if (cResp == null)
              Result.failure(CommandError.of(CommandError.Reason.Timeout,
                                             "Timed out waiting for response from Command Processor"))
            else
              cResp.sequenceResult()

          import scala.collection.JavaConverters._
          val result =
            sequenceResult
              .fold[Either[SagaError, Unit]](
                nelE => Left(SagaError.of(nelE.head().getMessage, nelE.tail().asScala.map(_.getMessage): _*)),
                _ => Right(()))

          ActionResponse(sagaId = aReq.sagaId,
                         actionId = aReq.actionId,
                         commandId = aReq.actionCommand.commandId,
                         result = result)
      }
      .selectKey[UUID]((_: UUID, sagaId: ActionResponse) => sagaId.sagaId)
      .peek(logValues[UUID, ActionResponse]("resultStream"))

    actionResponse
  }
}
