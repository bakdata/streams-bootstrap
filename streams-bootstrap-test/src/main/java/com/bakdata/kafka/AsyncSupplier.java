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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Call a {@link Supplier} asynchronously and wait for the result.
 *
 * @param <T> type of result
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsyncSupplier<T> {
    private final @NonNull CompletableFuture<T> future;

    /**
     * Call a supplier asynchronously. Execution starts immediately. Result can be awaited.
     * @param supplier supplier to call
     * @param <T> type of result
     * @return async supplier for awaiting result
     */
    public static <T> AsyncSupplier<T> getAsync(final Supplier<T> supplier) {
        final CompletableFuture<T> future = CompletableFuture.supplyAsync(supplier);
        return new AsyncSupplier<>(future);
    }

    static <T> T await(final CompletableFuture<T> future, final Duration timeout) throws InterruptedException {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new ExecutionException(e);
        } catch (final java.util.concurrent.TimeoutException e) {
            throw new TimeoutException(e);
        }
    }

    /**
     * Await the result of the supplier. This method blocks until the supplier has finished or the timeout has elapsed.
     * {@link RuntimeException RuntimeExceptions} are rethrown.
     *
     * @param timeout time to wait for result
     * @return result of the supplier
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public T await(final Duration timeout) throws InterruptedException {
        return await(this.future, timeout);
    }
}
