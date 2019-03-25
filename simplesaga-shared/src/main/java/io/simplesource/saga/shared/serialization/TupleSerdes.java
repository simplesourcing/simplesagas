package io.simplesource.saga.shared.serialization;

import io.simplesource.kafka.internal.util.Tuple2;
import io.simplesource.saga.shared.avro.generated.AvroTuple2;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class TupleSerdes {
    private static DatumWriter<AvroTuple2> datumWriter = new SpecificDatumWriter<>(AvroTuple2.SCHEMA$);
    private static DatumReader<AvroTuple2> datumReader = new SpecificDatumReader<>(AvroTuple2.SCHEMA$);

    private static <A, B> Serializer<Tuple2<A, B>> tuple2Serializer(final Serializer<A> serA, final Serializer<B> serB) {
        return new Serializer<Tuple2<A, B>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                serA.configure(configs, isKey);
                serB.configure(configs, isKey);
            }

            @Override
            public byte[] serialize(String topic, Tuple2<A, B> data) {
                byte[] bytesA = serA.serialize(topic + ".1", data.v1());
                byte[] bytesB = serB.serialize(topic + ".2", data.v2());

                AvroTuple2 tuple2 = new AvroTuple2(ByteBuffer.wrap(bytesA), ByteBuffer.wrap(bytesB));


                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

                try {
                    datumWriter.write(tuple2, encoder);
                    encoder.flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                return stream.toByteArray();
            }

            @Override
            public void close() {
                serA.close();
                serB.close();
            }
        };
    }

    private static <A, B> Deserializer<Tuple2<A, B>> tuple2Deserializer(final Deserializer<A> deA, final Deserializer<B> deB) {
        return new Deserializer<Tuple2<A, B>>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                deA.configure(configs, isKey);
                deB.configure(configs, isKey);
            }

            @Override
            public Tuple2<A, B> deserialize(String topic, byte[] data) {
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                try {
                    AvroTuple2 record = datumReader.read(null, decoder);
                    byte[] bytesA = record.getV1().array();
                    byte[] bytesB = record.getV2().array();
                    A a = deA.deserialize(topic + "_1", bytesA);
                    B b = deB.deserialize(topic + "_2", bytesB);
                    return new Tuple2<>(a, b);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                deA.close();
                deB.close();
            }
        };
    }

    public static <A, B> Serde<Tuple2<A, B>> tuple2(final Serde<A> serdeA, final Serde<B> serdeB) {
        return new Serde<Tuple2<A, B>>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                serdeA.configure(configs, isKey);
                serdeB.configure(configs, isKey);
            }

            @Override
            public void close() {
                serdeA.close();
                serdeB.close();

            }

            @Override
            public Serializer<Tuple2<A, B>> serializer() {
                return tuple2Serializer(serdeA.serializer(), serdeB.serializer());
            }

            @Override
            public Deserializer<Tuple2<A, B>> deserializer() {
                return tuple2Deserializer(serdeA.deserializer(), serdeB.deserializer());
            }
        };
    }
}
