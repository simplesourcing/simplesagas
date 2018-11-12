package action.common
import org.apache.kafka.streams.kstream.ForeachAction
import org.slf4j.Logger

object Utils {
  def logValues[K, V](logger: Logger, prefix: String): ForeachAction[K, V] =
    (k: K, v: V) => logger.info(s"$prefix: ${k.toString.take(6)}=$v")
}
