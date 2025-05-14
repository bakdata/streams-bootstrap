/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Call a {@link Runnable} asynchronously and wait for the result.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsyncRunnable {
    private final @NonNull CompletableFuture<Void> future;

    /**
     * Call a runnable asynchronously. Execution starts immediately. Execution can be awaited.
     *
     * @param runnable runnable to call
     * @return async runnable for awaiting execution
     */
    public static AsyncRunnable runAsync(final Runnable runnable) {
        final CompletableFuture<Void> future = CompletableFuture.runAsync(runnable);
        return new AsyncRunnable(future);
    }

    /**
     * Await the execution of the runnable. This method blocks until the runnable has finished or the timeout has
     * elapsed. {@link RuntimeException RuntimeExceptions} are rethrown.
     *
     * @param timeout time to wait for result
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void await(final Duration timeout) throws InterruptedException, ExecutionException {
        AsyncSupplier.await(this.future, timeout);
    }

}
