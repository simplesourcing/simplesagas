package io.simplesource.saga.serialization.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import io.simplesource.saga.model.action.ActionCommand;
import io.simplesource.saga.model.messages.ActionRequest;
import io.simplesource.saga.model.messages.ActionResponse;
import io.simplesource.saga.model.saga.SagaError;
import io.simplesource.saga.model.serdes.ActionSerdes;
import io.simplesource.saga.serialization.avro.generated.*;
import io.simplesource.saga.serialization.utils.SerdeUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

class AvroSerdesTest {
}
