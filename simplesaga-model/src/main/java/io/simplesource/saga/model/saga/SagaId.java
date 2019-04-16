package io.simplesource.saga.model.saga;

import lombok.Value;

import java.util.UUID;

/**
 * A saga id uniquely identifies the saga. It is also used as the topic key for most of the saga topics, including the
 * saga request and response, the saga state and state transition topics, as well as the action request and response topics.
 */
@Value(staticConstructor = "of")
public final class SagaId {
    /**
     * The d is represented internally as a UUID.
     */
    public final UUID id;

    /**
     * Generates a randon saga ID.
     *
     * @return the saga id
     */
    public static SagaId random() {
        return new SagaId(UUID.randomUUID());
    }

    /**
     * Converts a saga from its string representation (as a UUID string) to a saga id.
     *
     * @param uuidString the uuid string
     * @return the saga id
     */
    public static SagaId fromString(String uuidString) { return new SagaId(UUID.fromString(uuidString)); }
    @Override public String toString() { return id.toString(); }
}
