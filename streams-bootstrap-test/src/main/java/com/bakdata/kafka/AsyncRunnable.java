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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Call a {@link Runnable} asynchronously and wait for the result.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsyncRunnable {
    private final @NonNull AsyncSupplier<Void> supplier;

    /**
     * Call a runnable asynchronously. Execution starts immediately. Execution can be awaited.
     *
     * @param runnable runnable to call
     * @return async runnable for awaiting execution
     */
    public static AsyncRunnable runAsync(final Runnable runnable) {
        final Supplier<Void> supplier = asSupplier(runnable);
        return new AsyncRunnable(AsyncSupplier.getAsync(supplier));
    }

    private static Supplier<Void> asSupplier(final Runnable runnable) {
        return () -> {
            runnable.run();
            return null;
        };
    }

    /**
     * Await the execution of the runnable. This method blocks until the runnable has finished or the timeout has
     * elapsed. {@link RuntimeException RuntimeExceptions} are rethrown.
     *
     * @param timeout time to wait for result
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if the timeout has elapsed while waiting
     * @throws ExecutionException if a non-runtime exception was thrown by the runnable
     */
    public void await(final Duration timeout) throws InterruptedException, TimeoutException, ExecutionException {
        this.supplier.await(timeout);
    }
}
