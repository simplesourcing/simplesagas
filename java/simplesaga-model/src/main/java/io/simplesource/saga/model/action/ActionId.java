package io.simplesource.saga.model.action;

import lombok.Value;

import java.util.UUID;

@Value(staticConstructor = "of")
public final class ActionId {
    public final UUID id;

    public static ActionId random() {
        return new ActionId(UUID.randomUUID());
    }
}
