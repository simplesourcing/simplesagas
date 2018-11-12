package action
import http.{HttpRequest, HttpVerb}
import requests.RequestBlob
import io.circe._
import io.circe.parser._
import io.circe.syntax._

import scala.concurrent.{ExecutionContext, Future}

object HttpClient {
  final case class HttpError(statusCode: Int, statusMessage: String) extends Throwable(statusMessage)

  def requester[K, B: Decoder: Encoder, O: Decoder](
      implicit ec: ExecutionContext): HttpRequest[K, B] => Future[O] = r => {
    val httpAction = r.verb match {
      case HttpVerb.Get    => requests.get
      case HttpVerb.Post   => requests.post
      case HttpVerb.Put    => requests.put
      case HttpVerb.Delete => requests.delete
    }
    val data = r.body.fold[RequestBlob](RequestBlob.EmptyRequestBlob)(b =>
      RequestBlob.StringRequestBlob(b.asJson.noSpaces))
    Future(httpAction(url = r.url, data = data))
      .map { resp =>
        if (resp.statusCode >= 300) {
          throw HttpError(resp.statusCode, s"Error in Http Request: ${resp.statusMessage}")
        }
        parse(resp.data.text).flatMap(_.as[O]).toTry.get
      }
  }
}
