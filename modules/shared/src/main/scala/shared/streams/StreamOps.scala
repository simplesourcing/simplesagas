package shared.streams
import org.apache.kafka.streams.kstream._
import java.lang.Iterable

import org.apache.kafka.streams.KeyValue

import scala.collection.JavaConverters._

trait StreamOps {
  implicit class StreamOps[K, V](s: KStream[K, V]) {
    def _mapValues[VR](f: V => VR): KStream[K, VR] = {
      val valueMapper: ValueMapper[V, VR] = v => f(v)
      s.mapValues[VR](valueMapper)
    }

    def _mapValuesWithKey[VR](f: (K, V) => VR): KStream[K, VR] = {
      val valueMapperWithKey: ValueMapperWithKey[K, V, VR] = (k, v) => f(k, v)
      s.mapValues[VR](valueMapperWithKey)
    }

    def _map[KR, VR](f: (K, V) => (KR, VR)): KStream[KR, VR] = {
      val valueMapperWithKey: KeyValueMapper[K, V, KeyValue[KR, VR]] = (k, v) => {
        val fkv = f(k, v)
        KeyValue.pair(fkv._1, fkv._2)
      }
      s.map[KR, VR](valueMapperWithKey)
    }

    def _filter(f: V => Boolean): KStream[K, V] = {
      val p: Predicate[K, V] = (_, x) => f(x)
      s.filter(p)
    }

    def _filterWithKey(f: (K, V) => Boolean): KStream[K, V] = {
      val p: Predicate[K, V] = (k, v) => f(k, v)
      s.filter(p)
    }

    def _collect[VR](pf: PartialFunction[V, VR]): KStream[K, VR] = {
      _mapValues(v => pf.lift(v))
        ._filter(_.isDefined)
        ._mapValues(_.get)
    }

    def _collectWithKey[VR](pf: PartialFunction[V, VR]): KStream[K, VR] = {
      _mapValues[Option[VR]](v => pf.lift(v))
        ._filter(_.isDefined)
        ._mapValues(_.get)
    }

    def _flatMapValues[VR](f: V => Seq[VR]): KStream[K, VR] = {
      val valueMapper: ValueMapper[V, Iterable[VR]] = v => f(v).asJava
      s.flatMapValues(valueMapper)
    }

    def _flatMapValuesWithKey[VR](f: (K, V) => Seq[VR]): KStream[K, VR] = {
      val valueMapper: ValueMapperWithKey[K, V, Iterable[VR]] = (k, v) => f(k, v).asJava
      s.flatMapValues(valueMapper)
    }
  }
}
