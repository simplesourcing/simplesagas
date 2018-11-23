package model
import java.util.UUID

import model.messages.{SagaRequest, SagaResponse}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

object api {
  trait SagaAPI[A] {
    def submitSaga(request: SagaRequest[A]): Future[UUID]
    def getSagaResponse(requestId: UUID, timeout: Duration): Future[SagaResponse]
  }
}
