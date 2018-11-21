package action.async
import java.util.UUID

import action.common.{ActionProducer, IdempotentStream, Utils}
import model.messages.{ActionRequest, ActionResponse}
import org.apache.kafka.streams.kstream.{ForeachAction, KStream}
import org.slf4j.LoggerFactory

object AsyncStream {
  private val logger = LoggerFactory.getLogger("AsyncStream")

  def logValues[K, V](prefix: String): ForeachAction[K, V] = Utils.logValues[K, V](logger, prefix)

  def addSubTopology[A, I, K, O, R](ctx: AsyncContext[A, I, K, O, R],
                                    actionRequest: KStream[UUID, ActionRequest[A]],
                                    actionResponse: KStream[UUID, ActionResponse]): Unit = {
    // join the action request with corresponding prior command responses
    val idempotentAction =
      IdempotentStream.getActionRequestsWithResponse(ctx.actionSpec,
                                                     actionRequest,
                                                     actionResponse,
                                                     ctx.asyncSpec.actionType)

    // publish to output topics
    ActionProducer.actionResponse(ctx.actionSpec, ctx.actionTopicNamer, idempotentAction.priorResponses)
    ActionProducer.actionRequest(ctx.actionSpec,
                                 ctx.actionTopicNamer,
                                 idempotentAction.unprocessedRequests,
                                 unprocessed = true)
  }
}
