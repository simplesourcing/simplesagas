package http

import action.async.{AsyncApp, AsyncOutput, AsyncSpec}

import scala.concurrent.ExecutionContext

trait HttpOps {
  implicit class HttpApp[A](asyncApp: AsyncApp[A]) {

    def addHttpProcessor[K, B, O, R](httpSpec: HttpSpec[A, K, B, O, R])(
        implicit ec: ExecutionContext): AsyncApp[A] = {
      val asyncSpec = AsyncSpec[A, HttpRequest[K, B], K, O, R](
        httpSpec.actionType,
        httpSpec.requestDecoder,
        _.key,
        httpSpec.httpClient,
        httpSpec.groupId,
        httpSpec.outputSpec.map(o => AsyncOutput(o.resultDecoder, o.serdes, _.topicName, o.topicCreation))
      )
      asyncApp.addAsync(asyncSpec)
    }
  }
}
