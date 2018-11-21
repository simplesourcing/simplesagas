package topics.serdes

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.simplesource.api.CommandError
import io.simplesource.data.{NonEmptyList, Result, Sequence}
import io.simplesource.kafka.api.{AggregateSerdes, CommandSerdes}
import io.simplesource.kafka.model._
import io.simplesource.kafka.serialization.util.GenericSerde
import model.serdes.{ActionSerdes, SagaSerdes}
import model.messages._
import model.saga.Saga
import org.apache.kafka.common.serialization.{Serde, Serdes => KafkaSerdes}
import org.slf4j.LoggerFactory

import collection.JavaConverters._

object JsonSerdes {
  private val logger = LoggerFactory.getLogger("JsonSerdes")

  private implicit class CodecPairOps[B](codec: (Encoder[B], Decoder[B])) {
    def asSerde: Serde[B] = {
      implicit val encoder = codec._1
      implicit val decoder = codec._2
      serdeFromCodecs
    }
  }

  def serdeFromCodecs[B](implicit e: Encoder[B], d: Decoder[B]): Serde[B] = {
    import io.circe.parser._
    import io.circe.syntax._
    def toB: java.util.function.Function[String, B] =
      x =>
        parse(x)
          .flatMap(j => j.as[B])
          .fold(err => {
            logger.error(s"Error on:\n$x \n$err")
            throw err
          }, identity)
    def fromB: java.util.function.Function[B, String] = _.asJson.noSpaces

    GenericSerde.of[B, String](KafkaSerdes.String(), fromB, toB)
  }

  private def productCodecs2[A0: Encoder: Decoder, A1: Encoder: Decoder, B](n0: String, n1: String)(
      b2p: B => (A0, A1),
      p2b: (A0, A1) => B): (Encoder[B], Decoder[B]) = {
    val encoder: Encoder[B] = Encoder.forProduct2(n0, n1)(b2p)
    val decoder: Decoder[B] =
      Decoder.forProduct2(n0, n1)((a0: A0, a1: A1) => p2b(a0, a1))

    (encoder, decoder)
  }

  private def productCodecs3[A0: Encoder: Decoder, A1: Encoder: Decoder, A2: Encoder: Decoder, B](
      n0: String,
      n1: String,
      n2: String)(b2p: B => (A0, A1, A2), p2b: (A0, A1, A2) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct3(n0, n1, n2)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct3(n0, n1, n2)((a0: A0, a1: A1, a2: A2) => p2b(a0, a1, a2))

    (encoder, decoder)
  }

  private def productCodecs4[A0: Encoder: Decoder,
                             A1: Encoder: Decoder,
                             A2: Encoder: Decoder,
                             A3: Encoder: Decoder,
                             B](n0: String, n1: String, n2: String, n3: String)(
      b2p: B => (A0, A1, A2, A3),
      p2b: (A0, A1, A2, A3) => B): (Encoder[B], Decoder[B]) = {
    implicit val encoder: Encoder[B] = Encoder.forProduct4(n0, n1, n2, n3)(b2p)
    implicit val decoder: Decoder[B] =
      Decoder.forProduct4(n0, n1, n2, n3)((a0: A0, a1: A1, a2: A2, a3: A3) => p2b(a0, a1, a2, a3))

    (encoder, decoder)
  }

  object ResultParts {
    implicit val cee: Encoder[CommandError] =
      implicitly[Encoder[(String, String)]]
        .contramap[CommandError](ce => (ce.getReason.toString, ce.getMessage))
    implicit val ced: Decoder[CommandError] =
      implicitly[Decoder[(String, String)]]
        .map(s => CommandError.of(CommandError.Reason.valueOf(s._1), s._2))

    final case class LWrapper(errors: List[CommandError])
    implicit val nele: Encoder[NonEmptyList[CommandError]] =
      io.circe.generic.semiauto
        .deriveEncoder[LWrapper]
        .contramapObject(nel => LWrapper(nel.head() :: nel.tail().asScala.toList))
    implicit val neld: Decoder[NonEmptyList[CommandError]] =
      io.circe.generic.semiauto
        .deriveDecoder[LWrapper]
        .map(l => NonEmptyList.fromList(l.errors.asJava))

    def au[A: Encoder: Decoder] =
      productCodecs2[A, Long, AggregateUpdate[A]]("aggregate", "sequence")(
        v => (v.aggregate(), v.sequence().getSeq),
        (v, s) => new AggregateUpdate(v, Sequence.position(s))
      )

    def cr: Serde[CommandResponse] = {

      type ErrorOrSequence =
        Either[NonEmptyList[CommandError], Sequence]

      implicit def seqE(implicit enc: Encoder[Long]): Encoder[Sequence] = enc.contramap(_.getSeq)
      implicit def seqD(implicit enc: Decoder[Long]): Decoder[Sequence] = enc.map(Sequence.position)

      implicit def rese: Encoder[Result[CommandError, Sequence]] =
        io.circe.generic.semiauto
          .deriveEncoder[ErrorOrSequence]
          .contramapObject(r => {
            if (r.isSuccess)
              Right[NonEmptyList[CommandError], Sequence](r.getOrElse(null))
            else
              Left[NonEmptyList[CommandError], Sequence](r.failureReasons().get())
          })
      implicit def resd: Decoder[Result[CommandError, Sequence]] =
        io.circe.generic.semiauto
          .deriveDecoder[ErrorOrSequence]
          .map({
            case Right(r) =>
              Result.success[CommandError, Sequence](r)
            case Left(e) =>
              Result.failure[CommandError, Sequence](e)
          })

      productCodecs3[UUID, Long, Result[CommandError, Sequence], CommandResponse]("commandId",
                                                                                  "readSequence",
                                                                                  "sequenceResult")(
        x => (x.commandId(), x.readSequence().getSeq, x.sequenceResult()),
        (id, seq, ur) => new CommandResponse(id, Sequence.position(seq), ur))
    }.asSerde

  }

  def aggregateSerdes[K: Encoder: Decoder, C: Encoder: Decoder, E: Encoder: Decoder, A: Encoder: Decoder]
    : AggregateSerdes[K, C, E, A] =
    new AggregateSerdes[K, C, E, A] {

      val aks = serdeFromCodecs[K]

      val crs =
        productCodecs4[K, C, Long, UUID, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.aggregateKey(), v.command(), v.readSequence().getSeq, v.commandId()),
          (k, v, rs, id) => new CommandRequest(k, v, Sequence.position(rs), id)
        ).asSerde

      val crks = serdeFromCodecs[UUID]

      val vwss =
        productCodecs2[E, Long, ValueWithSequence[E]]("value", "sequence")(
          v => (v.value(), v.sequence().getSeq),
          (v, s) => new ValueWithSequence(v, Sequence.position(s))
        ).asSerde

      val au = ResultParts.au[A]

      val ur = {
        implicit val aue: Encoder[AggregateUpdate[A]] = au._1
        implicit val aud: Decoder[AggregateUpdate[A]] = au._2

        import ResultParts._

        type ErrorOrUpdate =
          Either[NonEmptyList[CommandError], AggregateUpdate[A]]

        implicit def rese: Encoder[Result[CommandError, AggregateUpdate[A]]] =
          io.circe.generic.semiauto
            .deriveEncoder[ErrorOrUpdate]
            .contramapObject(r => {
              if (r.isSuccess)
                Right[NonEmptyList[CommandError], AggregateUpdate[A]](r.getOrElse(null))
              else
                Left[NonEmptyList[CommandError], AggregateUpdate[A]](r.failureReasons().get())
            })
        implicit def resd: Decoder[Result[CommandError, AggregateUpdate[A]]] =
          io.circe.generic.semiauto
            .deriveDecoder[ErrorOrUpdate]
            .map({
              case Right(r) =>
                Result.success[CommandError, AggregateUpdate[A]](r)
              case Left(e) =>
                Result.failure[CommandError, AggregateUpdate[A]](e)
            })

        productCodecs3[UUID, Long, Result[CommandError, AggregateUpdate[A]], AggregateUpdateResult[A]](
          "commandId",
          "readSequence",
          "updatedAggregateResult")(
          x => (x.commandId(), x.readSequence().getSeq, x.updatedAggregateResult()),
          (id, seq, ur) => new AggregateUpdateResult(id, Sequence.position(seq), ur))
      }.asSerde

      val aus = au.asSerde
      val cr  = ResultParts.cr

      override def aggregateKey(): Serde[K]                         = aks
      override def commandRequest(): Serde[CommandRequest[K, C]]    = crs
      override def commandResponseKey(): Serde[UUID]                = crks
      override def valueWithSequence(): Serde[ValueWithSequence[E]] = vwss
      override def aggregateUpdate(): Serde[AggregateUpdate[A]]     = aus
      override def updateResult(): Serde[AggregateUpdateResult[A]]  = ur
      override def commandResponse(): Serde[CommandResponse]        = cr
    }

  def commandSerdes[K: Encoder: Decoder, C: Encoder: Decoder]: CommandSerdes[K, C] =
    new CommandSerdes[K, C] {

      val aks = serdeFromCodecs[K]

      val crs =
        productCodecs4[K, C, Long, UUID, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
          v => (v.aggregateKey(), v.command(), v.readSequence().getSeq, v.commandId()),
          (k, v, rs, id) => new CommandRequest(k, v, Sequence.position(rs), id)
        ).asSerde

      val crks = serdeFromCodecs[UUID]
      val cr   = ResultParts.cr

      override def aggregateKey(): Serde[K]                      = aks
      override def commandRequest(): Serde[CommandRequest[K, C]] = crs
      override def commandResponseKey(): Serde[UUID]             = crks
      override def commandResponse(): Serde[CommandResponse]     = cr
    }

  def actionSerdes[A: Encoder: Decoder]: ActionSerdes[A] = new ActionSerdes[A] {
    import io.circe.generic.auto._

    override lazy val uuid: Serde[UUID] = serdeFromCodecs[UUID]

    override lazy val request: Serde[ActionRequest[A]] = serdeFromCodecs[ActionRequest[A]]

    override lazy val response: Serde[ActionResponse] = serdeFromCodecs[ActionResponse]
  }

  def sagaSerdes[A: Encoder: Decoder]: SagaSerdes[A] = new SagaSerdes[A] {
    import io.circe.generic.auto._
    override lazy val uuid: Serde[UUID]                         = serdeFromCodecs[UUID]
    override lazy val request: Serde[SagaRequest[A]]            = serdeFromCodecs[SagaRequest[A]]
    override lazy val response: Serde[SagaResponse]             = serdeFromCodecs[SagaResponse]
    override lazy val state: Serde[Saga[A]]                     = serdeFromCodecs[Saga[A]]
    override lazy val transition: Serde[SagaStateTransition[A]] = serdeFromCodecs[SagaStateTransition[A]]
  }
}
