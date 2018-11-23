package model

import java.util.UUID

import model.messages._
import model.saga.Saga
import org.apache.kafka.common.serialization.Serde

object serdes {
  trait SagaClientSerdes[A] {
    def uuid: Serde[UUID]
    def request: Serde[SagaRequest[A]]
    def response: Serde[SagaResponse]
  }

  trait SagaSerdes[A] extends SagaClientSerdes[A] {
    def state: Serde[Saga[A]]
    def transition: Serde[SagaStateTransition[A]]
  }

  trait ActionSerdes[A] {
    def uuid: Serde[UUID]
    def request: Serde[ActionRequest[A]]
    def response: Serde[ActionResponse]
  }
}
