package io.simplesource.saga.action.async;

import io.simplesource.data.Result;

import java.util.function.Consumer;

public interface CallBackProvider<O> {
    Consumer<Result<Throwable, O>> resultConsumer();
}
