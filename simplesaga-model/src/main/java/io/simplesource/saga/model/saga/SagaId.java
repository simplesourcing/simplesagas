package io.simplesource.saga.model.saga;

import lombok.Value;

import java.util.UUID;

/**
 * The type Saga id.
 */
@Value(staticConstructor = "of")
public final class SagaId {
    /**
     * The Id.
     */
    public final UUID id;

    /**
     * Random saga id.
     *
     * @return the saga id
     */
    public static SagaId random() {
        return new SagaId(UUID.randomUUID());
    }

    /**
     * From string saga id.
     *
     * @param uuidString the uuid string
     * @return the saga id
     */
    public static SagaId fromString(String uuidString) { return new SagaId(UUID.fromString(uuidString)); }
    @Override public String toString() { return id.toString(); }
}
