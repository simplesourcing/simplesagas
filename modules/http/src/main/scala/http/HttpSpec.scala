package http

import action.async.AsyncSerdes
import scala.concurrent.Future

sealed trait HttpVerb
object HttpVerb {
  case object Get    extends HttpVerb
  case object Post   extends HttpVerb
  case object Put    extends HttpVerb
  case object Delete extends HttpVerb
}

/**
  * @tparam K - key for the output topic
  * @tparam B - body for Http request
  */
final case class HttpRequest[K, B](key: K,
                                   verb: HttpVerb,
                                   url: String,
                                   headers: Map[String, String],
                                   body: Option[B],
                                   topicName: Option[String])

/**
  * @param resultDecoder
  * @param serdes
  * @param topicNames
  * @tparam K - key for the output topic
  * @tparam O - output returned by the Http request - also normally quite generic
  * @tparam R - final result type that ends up in output topic
  */
final case class HttpOutput[K, O, R](resultDecoder: O => Option[Either[Throwable, R]],
                                     serdes: AsyncSerdes[K, R],
                                     topicNames: List[String])

/**
  * @param actionType
  * @param requestDecoder
  * @param groupId
  * @param outputSpec
  * @tparam A - common representation type for all action commands (typically Json / GenericRecord for Avro)
  * @tparam K - key for the output topic
  * @tparam B - body for Http request
  * @tparam O - output returned by the Http request - also normally quite generic
  * @tparam R - final result type that ends up in output topic
  */
final case class HttpSpec[A, K, B, O, R](actionType: String,
                                         requestDecoder: A => Either[Throwable, HttpRequest[K, B]],
                                         httpClient: HttpRequest[K, B] => Future[O],
                                         groupId: String,
                                         outputSpec: Option[HttpOutput[K, O, R]])
