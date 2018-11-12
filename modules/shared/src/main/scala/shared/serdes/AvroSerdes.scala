package shared.serdes

import com.sksamuel.avro4s.RecordFormat
import io.simplesource.kafka.serialization.util.GenericMapper
import org.apache.avro.generic.GenericRecord

object AvroSerdes {
  implicit class RecordFormatOps[A](recordFormat: RecordFormat[A]) {
    def genericMapper: GenericMapper[A, GenericRecord] =
      new GenericMapper[A, GenericRecord] {
        override def toGeneric(v: A): GenericRecord   = recordFormat.to(v)
        override def fromGeneric(g: GenericRecord): A = recordFormat.from(g)
      }
  }
}
