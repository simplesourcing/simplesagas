package io.simplesource.saga.action.async;

import io.simplesource.data.Result;

import java.util.function.Consumer;

public interface Callback<O> {
    void complete(Result<Throwable, O> result);
}
