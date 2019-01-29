package io.simplesource.saga.serialization.avro;

import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.model.serdes.SagaSerdes;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SagaInternalSerdesTest {

    private static String SCHEMA_URL = "http://localhost:8081/";
    private static String FAKE_TOPIC = "topic";

    @Test
    void sagaStateTest() {
        SagaSerdes<GenericRecord> serdes = AvroSerdes.sagaSerdes(SCHEMA_URL, true);

        Saga<GenericRecord> original = SagaTestUtils.getTestSaga();

        byte[] serialized = serdes.state().serializer().serialize(FAKE_TOPIC, original);
        Saga<GenericRecord> deserialized = serdes.state().deserializer().deserialize(FAKE_TOPIC, serialized);

        String originalAsString = original.toString();
        assertThat(deserialized.toString()).isEqualTo(originalAsString);
    }

}
