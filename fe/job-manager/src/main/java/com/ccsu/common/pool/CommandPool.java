package com.ccsu.common.pool;

import java.util.concurrent.CompletableFuture;

public interface CommandPool extends AutoCloseable {

    interface Command<T> {
        /**
         * The execution logic of the command.
         *
         * @return command to execute.
         */
        T execute();
    }

    <V> CompletableFuture<V> submit(
            String descriptor,
            Command<V> command,
            boolean runInSameThread);
}
