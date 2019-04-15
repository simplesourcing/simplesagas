package io.simplesource.saga.model.action;

import lombok.Value;

import java.util.UUID;

/**
 * An {@code ActionId} uniquely identifies an action in the saga dependency graph.
 */
@Value(staticConstructor = "of")
public final class ActionId {
    /**
     * The Id.
     */
    public final UUID id;

    /**
     * Returns an random {@code ActionId}
     *
     * @return the action id
     */
    public static ActionId random() {
        return new ActionId(UUID.randomUUID());
    }

    /**
     * Converts an action id from its string representation as a UUID string.
     *
     * @param uuidString the uuid string
     * @return the action id
     */
    public static ActionId fromString(String uuidString) { return new ActionId(UUID.fromString(uuidString)); }
    @Override public String toString() { return id.toString(); }
}
