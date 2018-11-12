package action.async

import scala.concurrent.Future

final case class AsyncOutput[I, K, O, R](outputDecoder: O => Option[Either[Throwable, R]],
                                         serdes: AsyncSerdes[K, R],
                                         topicName: I => Option[String],
                                         topicNames: List[String])

/**
  * @param actionType
  * @param inputDecoder
  * @param keyMapper
  * @param asyncFunction
  * @param groupId
  * @param outputSpec
  * @tparam A - common representation form for all action commands (typically Json / GenericRecord for Avro)
  * @tparam I - input to async function
  * @tparam K - key for the output topic
  * @tparam O - output returned by async function
  * @tparam R - final result type that ends up in output topic
  */
final case class AsyncSpec[A, I, K, O, R](actionType: String,
                                          inputDecoder: A => Either[Throwable, I],
                                          keyMapper: I => K,
                                          asyncFunction: I => Future[O],
                                          groupId: String,
                                          outputSpec: Option[AsyncOutput[I, K, O, R]])
