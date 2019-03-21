package io.simplesource.saga.shared.avro;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.shared.serialization.TupleSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;



import static org.assertj.core.api.Assertions.assertThat;


class ActionSerdesTest {
    private static String FAKE_TOPIC = "topic";
    @Test
    void tuple2Test() {
        Serde<Tuple2<Long, String>> tupleSerde = TupleSerdes.tuple2(Serdes.Long(), Serdes.String());

        byte[] serialized = tupleSerde.serializer().serialize(FAKE_TOPIC, Tuple2.of(42L, "Hello"));
        Tuple2<Long, String> deserialized = tupleSerde.deserializer().deserialize(FAKE_TOPIC, serialized);
        assertThat(deserialized.v1()).isEqualTo(42L);
        assertThat(deserialized.v2()).isEqualTo("Hello");

        // now do nested test for good measure
        Serde<Tuple2<Long, Tuple2<Long, String>>> nestedTupleSerde = TupleSerdes.tuple2(Serdes.Long(), tupleSerde);

        byte[] serializedNested = nestedTupleSerde.serializer().serialize(FAKE_TOPIC, Tuple2.of(42L, Tuple2.of(42L, "Hello")));
        Tuple2<Long, Tuple2<Long, String>> deserializedNested = nestedTupleSerde.deserializer().deserialize(FAKE_TOPIC, serializedNested);
        assertThat(deserializedNested.v2().v1()).isEqualTo(42L);
        assertThat(deserializedNested.v2().v2()).isEqualTo("Hello");
        assertThat(deserializedNested.v1()).isEqualTo(42L);
    }
}
