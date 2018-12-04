//package io.simplesource.saga.saga.app
//
//import java.util.UUID
//
//import model.messages.{ActionRequest, SagaResponse, SagaStateTransition}
//import model.saga.Saga
//import org.apache.kafka.streams.kstream.{KStream, Produced}
//import shared.topics.TopicTypes.{ActionTopic, SagaTopic}
//
//object SagaProducer {
//
//  def actionRequests[A](ctx: SagaContext[A], actionRequests: KStream[UUID, ActionRequest[A]]): Unit = {
//    actionRequests
//      .to(ctx.actionTopicNamer(ActionTopic.request),
//          Produced.`with`(ctx.aSpec.serdes.uuid, ctx.aSerdes.request))
//  }
//
//  def sagaState[A](ctx: SagaContext[A], sagaState: KStream[UUID, Saga[A]]): Unit = {
//    sagaState.to(ctx.sagaTopicNamer(SagaTopic.state),
//                 Produced.`with`(ctx.sSpec.serdes.uuid, ctx.sSerdes.state))
//  }
//
//  def sagaStateTransitions[A](ctx: SagaContext[A],
//                              stateTransitions: KStream[UUID, SagaStateTransition[A]]*): Unit = {
//    stateTransitions.foreach(
//      _.to(ctx.sagaTopicNamer(SagaTopic.stateTransition),
//           Produced.`with`(ctx.sSerdes.uuid, ctx.sSerdes.transition)))
//  }
//
//  def sagaResponses[A](ctx: SagaContext[A], sagaResponse: KStream[UUID, SagaResponse]) = {
//    sagaResponse.to(ctx.sagaTopicNamer(SagaTopic.response),
//                    Produced.`with`(ctx.sSpec.serdes.uuid, ctx.sSerdes.response))
//  }
//
//}
