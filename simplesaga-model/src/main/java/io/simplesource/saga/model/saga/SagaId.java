package io.simplesource.saga.model.saga;

import lombok.Value;

import java.util.UUID;

@Value(staticConstructor = "of")
public final class SagaId {
    public final UUID id;

    public static SagaId random() {
        return new SagaId(UUID.randomUUID());
    }
    public static SagaId fromString(String uuidString) { return new SagaId(UUID.fromString(uuidString)); }
    @Override public String toString() { return id.toString(); }
}
