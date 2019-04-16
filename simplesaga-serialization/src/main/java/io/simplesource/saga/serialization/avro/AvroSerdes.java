package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.model.serdes.SagaClientSerdes;
import io.simplesource.saga.model.serdes.SagaSerdes;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/**
 * Factory methods for Avro Serdes.
 *
 * Avro is the recommended format for serialization format for Saga applications. In addition, if your application is written in Java, it is suggested to create
 * to define your data types, and in particular the data representations of your action command types (the type {@code A}) using AVDL, and then set up the Avro Maven plugin to generate Java objects that correspond to your AVDL definitions. An
 * example of how to do this is available in the {@code pom.xml} file for this module. These generated classes are of type {@link SpecificRecord},
 * so you can choose {@code A} to be {@code SpecifiRecord}.
 * You can then use the {@link Specific AvroSerdes.Specific} methods to generate all the Serdes you need without having to do any custom marshalling.
 * <p>
 * If choose a different concrete type for {@code A}, you can use the methods of {@link AvroSerdes below}, but it does require you to provide an
 * appropriate {@code payloadSerde} for {@code A}, which may involve writing a little more boilerplate code.
 */
final public class AvroSerdes {

    /**
     * Create action serdes (serdes required for serializing to and from action request and response topics)
     *
     * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param payloadSerde          the payload serde required for serializing to and from the shared action command type {@code A}
     * @param schemaRegistryUrl     the schema registry url
     * @param useMockSchemaRegistry the use mock schema registry
     * @return the action serdes
     */
    public static <A> ActionSerdes<A> actionSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroActionSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    /**
     * Saga client serdes (serdes required for the saga request and response topics)
     *
     * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param payloadSerde          the payload serde required for serializing to and from the shared action command type {@code A}
     * @param schemaRegistryUrl     the schema registry url
     * @param useMockSchemaRegistry the use mock schema registry
     * @return the saga client serdes
     */
    public static <A> SagaClientSerdes<A> sagaClientSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroSagaClientSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    /**
     * Saga serdes (saga client topics, as well as the saga internal state and state transition topics)
     *
     * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is typically a generic type, such as Json, or if using Avro serialization, SpecificRecord or GenericRecord
     * @param payloadSerde          the payload serde required for serializing to and from the shared action command type {@code A}
     * @param schemaRegistryUrl     the schema registry url
     * @param useMockSchemaRegistry the use mock schema registry
     * @return the saga client serdes
     */
    public static <A> SagaSerdes<A> sagaSerdes(
            final Serde<A> payloadSerde,
            final String schemaRegistryUrl,
            final boolean useMockSchemaRegistry) {
        return new AvroSagaSerdes<>(payloadSerde, schemaRegistryUrl, useMockSchemaRegistry);
    }

    /**
     * Factory methods which can be used where {@code A} is a subclass of {@link SpecificRecord}. This includes all the types generated from AVDL by the Avro Maven plugin.
     */
    public interface Specific {

         static <A extends SpecificRecord> ActionSerdes<A> actionSerdes(
                final String schemaRegistryUrl) {
            return actionSerdes(schemaRegistryUrl, false);
        }

        /**
         * Create action serdes (serdes required for serializing to and from action request and response topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link SpecificRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the action serdes
         */
        static <A extends SpecificRecord> ActionSerdes<A> actionSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.actionSerdes(
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        /**
         * Saga client serdes (serdes required for the saga request and response topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link SpecificRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the saga client serdes
         */
        static <A extends SpecificRecord> SagaClientSerdes<A> sagaClientSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.sagaClientSerdes(
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        /**
         * Saga serdes (saga client topics, as well as the saga internal state and state transition topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link SpecificRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the saga client serdes
         */
        static <A extends SpecificRecord> SagaSerdes<A> sagaSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.sagaSerdes(
                    SpecificSerdeUtils.specificAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }
    }

    /**
     * Factory methods which can be used where {@code A} is a subclass of {@link GenericRecord}.
     */
    public interface Generic {
        /**
         * Create action serdes (serdes required for serializing to and from action request and response topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link GenericRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the action serdes
         */
        static <A extends GenericRecord> ActionSerdes<A> actionSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.actionSerdes(
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        /**
         * Saga client serdes (serdes required for the saga request and response topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link GenericRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the saga client serdes
         */
        static <A extends GenericRecord> SagaClientSerdes<A> sagaClientSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.sagaClientSerdes(
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }

        /**
         * Saga serdes (saga client topics, as well as the saga internal state and state transition topics)
         *
         * @param <A>                   a representation of an action command that is shared across all actions in the saga. This is must be a subclass of {@link GenericRecord}
         * @param schemaRegistryUrl     the schema registry url
         * @param useMockSchemaRegistry the use mock schema registry
         * @return the saga client serdes
         */
        static <A extends GenericRecord> SagaSerdes<A> sagaSerdes(
                final String schemaRegistryUrl,
                final boolean useMockSchemaRegistry) {
            SchemaRegistryClient regClient = useMockSchemaRegistry ? new MockSchemaRegistryClient() : null;
            return AvroSerdes.sagaSerdes(
                    GenericSerdeUtils.genericAvroSerde(schemaRegistryUrl, false, regClient),
                    schemaRegistryUrl,
                    useMockSchemaRegistry);
        }
    }

}
