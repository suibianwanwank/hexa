package com.ccsu.common.pool;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class CommandPoolImpl implements CommandPool {

    private final ExecutorService executorService;

    public CommandPoolImpl() {
        // TODO Canonical Thread Names
        this.executorService = newCachedThreadPool();
    }

    @Override
    public <V> CompletableFuture<V> submit(String descriptor, Command<V> command, boolean runInSameThread) {
        final long submittedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        CommandWrapper<V> commandWrapper = new CommandWrapper<>(command, descriptor, submittedTime);
        if (runInSameThread) {
            commandWrapper.run();
        } else {
            executorService.execute(commandWrapper);
        }
        return commandWrapper.getFuture();
    }

    @Override
    public void close() throws Exception {

    }

    static class CommandWrapper<T> implements Runnable {
        private final CompletableFuture<T> future = new CompletableFuture<>();
        private final String describe;
        private final long submittedTime;
        private final Command<T> command;

        CommandWrapper(
                Command<T> command,
                @Nullable String descriptor,
                long submittedTime) {
            this.command = requireNonNull(command, "command is null");
            this.describe = descriptor;
            this.submittedTime = submittedTime;
        }

        @Override
        public void run() {
            final long waitInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - submittedTime;
            T result = command.execute();
            future.complete(result);
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }
}
