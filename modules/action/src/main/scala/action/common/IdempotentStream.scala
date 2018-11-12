package action.common
import java.util.UUID

import model.messages.{ActionRequest, ActionResponse}
import model.specs.ActionProcessorSpec
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import shared.streams.syntax._

object IdempotentStream {
  final case class IdempotentAction[A](priorResponses: KStream[UUID, ActionResponse],
                                       unprocessedRequests: KStream[UUID, ActionRequest[A]])

  def getActionRequestsWithResponse[A, I, K, C](aSpec: ActionProcessorSpec[A],
                                                actionRequests: KStream[UUID, ActionRequest[A]],
                                                actionResponse: KStream[UUID, ActionResponse],
                                                actionType: String): IdempotentAction[A] = {
    val actionByCommandId: KTable[UUID, ActionResponse] = {
      val materializer: Materialized[UUID, ActionResponse, KeyValueStore[Bytes, Array[Byte]]] =
        Materialized
          .as[UUID, ActionResponse, KeyValueStore[Bytes, Array[Byte]]](
            "last_action_by_command_id_" + actionType)
          .withKeySerde(aSpec.serdes.uuid)
          .withValueSerde(aSpec.serdes.response)

      actionResponse
        .selectKey[UUID]((_, aResp) => aResp.commandId)
        .groupByKey(Serialized.`with`(aSpec.serdes.uuid, aSpec.serdes.response))
        .reduce((_: ActionResponse, cr2: ActionResponse) => cr2, materializer)
    }

    val valueJoiner: ValueJoiner[ActionRequest[A], ActionResponse, (ActionRequest[A], ActionResponse)] =
      (a, c) => (a, c)

    val actionRequestWithResponse: KStream[UUID, (ActionRequest[A], ActionResponse)] = actionRequests
      ._filter(aReq => aReq.actionType == actionType)
      .leftJoin(actionByCommandId,
                valueJoiner,
                Joined.`with`(aSpec.serdes.uuid, aSpec.serdes.request, aSpec.serdes.response))

    // split between unprocessed and prior-processed actions
    val processedResponses  = actionRequestWithResponse._collect { case (_, resp) if resp != null   => resp }
    val unprocessedRequests = actionRequestWithResponse._collect { case (req, resp) if resp == null => req }
    IdempotentAction(processedResponses, unprocessedRequests)
  }

}
