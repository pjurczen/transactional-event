package com.github.jonasrutishauser.transactional.event.quarkus.concurrent;

import io.vertx.core.impl.ContextInternal;

/**
 * Runs the delegate on a fresh duplicated Vert.x context.
 * <p>
 * Quarkus keeps the CDI request context in the local data of the current duplicated Vert.x context and its
 * {@code @VirtualThreads} executor propagates that context to the executing thread. Without a fresh context,
 * clearing the CDI context for event processing would overwrite (and later destroy) the request context of the
 * publisher. Quarkus does the same for its own worker pool, see {@code VertxCoreRecorder#executionContextHandler}.
 */
final class ContextIsolatingRunnable implements Runnable {

    private final Runnable delegate;

    ContextIsolatingRunnable(Runnable delegate) {
        this.delegate = delegate;
    }

    @Override
    public void run() {
        ContextInternal current = ContextInternal.current();
        if (current == null) {
            delegate.run();
            return;
        }
        ContextInternal fresh = current.duplicate();
        ContextInternal previous = fresh.beginDispatch();
        try {
            delegate.run();
        } finally {
            fresh.endDispatch(previous);
        }
    }

}
