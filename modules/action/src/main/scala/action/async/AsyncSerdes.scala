package action.async
import org.apache.kafka.common.serialization.Serde

final case class AsyncSerdes[K, R](key: Serde[K], output: Serde[R])
