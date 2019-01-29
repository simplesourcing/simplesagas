package io.simplesource.saga.user.action

import java.util.Optional
import java.util.function.BiConsumer

import io.circe.syntax._
import io.circe.parser.parse
import io.circe.{Decoder, Encoder}
import io.simplesource.data.Result
import io.simplesource.saga.action.async.Callback
import io.simplesource.saga.action.http.HttpRequest
import io.simplesource.saga.action.http.HttpRequest.HttpVerb
import requests.RequestBlob

import scala.concurrent.{ExecutionContext, Future}

object HttpClient {
  import io.simplesource.saga.scala.serdes.JavaCodecs._
  import io.simplesource.saga.scala.serdes.ProductCodecs._

  implicit def httpRequest[K: Encoder: Decoder, B: Encoder: Decoder]
    : (Encoder[HttpRequest[K, B]], Decoder[HttpRequest[K, B]]) = {
    productCodecs6[K,
                   String,
                   String,
                   java.util.Map[String, String],
                   Optional[B],
                   Optional[String],
                   HttpRequest[K, B]](
      "key",
      "verb",
      "url",
      "headers",
      "body",
      "topicName"
    )(r => (r.key, r.verb.toString, r.url, r.headers, r.body, r.topicName),
      (k, v, u, h, b, t) => new HttpRequest[K, B](k, HttpVerb.valueOf(v), u, h, b, t))
  }

  final case class HttpError(statusCode: Int, statusMessage: String) extends Throwable(statusMessage)

  def requester[K, B: Decoder: Encoder, O: Decoder](
      implicit ec: ExecutionContext): BiConsumer[HttpRequest[K, B], Callback[O]] = (r, callBack) => {
    val httpAction = r.verb match {
      case HttpVerb.Get    => requests.get
      case HttpVerb.Post   => requests.post
      case HttpVerb.Put    => requests.put
      case HttpVerb.Delete => requests.delete
    }
    val data = r.body
      .map[RequestBlob](b => RequestBlob.StringRequestBlob(b.asJson.noSpaces))
      .orElse(RequestBlob.EmptyRequestBlob)
    httpAction(url = r.url, data = data)
    Future(httpAction(url = r.url, data = data))
      .map { resp =>
        if (resp.statusCode >= 300) {
          throw HttpError(resp.statusCode, s"Error in Http Request: ${resp.statusMessage}")
        }
        parse(resp.data.text)
          .flatMap(_.as[O])
          .fold(e => Result.failure(e), a => Result.success(a))
      }
      .map[Result[Throwable, O]](_.errorMap[Throwable](e => e).map(x => x))
      .onComplete(tryRes =>
        tryRes.fold[Int](e => { callBack.complete(Result.failure(e)); 0 }, r => { callBack.complete(r); 0 }))
  }
}
