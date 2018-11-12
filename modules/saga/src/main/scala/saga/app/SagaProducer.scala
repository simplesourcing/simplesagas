package saga.app
import java.util.UUID

import model.messages.{ActionRequest, SagaResponse, SagaStateTransition}
import model.saga.Saga
import model.topics.{ActionTopic, SagaTopic}
import org.apache.kafka.streams.kstream.{KStream, Produced}

object SagaProducer {

  def actionRequests[A](ctx: SagaContext[A], actionRequests: KStream[UUID, ActionRequest[A]]): Unit = {
    actionRequests
      .to(ctx.aSpec.topicNamer(ActionTopic.request),
          Produced.`with`(ctx.aSpec.serdes.uuid, ctx.aSerdes.request))
  }

  def sagaState[A](ctx: SagaContext[A], sagaState: KStream[UUID, Saga[A]]): Unit = {
    sagaState.to(ctx.sSpec.topicNamer(SagaTopic.state),
                 Produced.`with`(ctx.sSpec.serdes.uuid, ctx.sSerdes.state))
  }

  def sagaStateTransitions[A](ctx: SagaContext[A],
                              stateTransitions: KStream[UUID, SagaStateTransition[A]]*): Unit = {
    stateTransitions.foreach(
      _.to(ctx.sSpec.topicNamer(SagaTopic.stateTransition),
           Produced.`with`(ctx.sSerdes.uuid, ctx.sSerdes.transition)))
  }

  def sagaResponses[A](ctx: SagaContext[A], sagaResponse: KStream[UUID, SagaResponse]) = {
    sagaResponse.to(ctx.sSpec.topicNamer(SagaTopic.response),
                    Produced.`with`(ctx.sSpec.serdes.uuid, ctx.sSerdes.response))
  }

}
