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

import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * Call a {@link Supplier} asynchronously and wait for the result.
 *
 * @param <T> type of result
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsyncSupplier<T> {
    private final @NonNull CountDownLatch shutdown;
    private final @NonNull CapturingUncaughtExceptionHandler exceptionHandler;
    private final @NonNull ResultProvider<T> resultProvider;

    /**
     * Call a supplier asynchronously. Execution starts immediately. Result can be awaited.
     * @param supplier supplier to call
     * @param <T> type of result
     * @return async supplier for awaiting result
     */
    public static <T> AsyncSupplier<T> getAsync(final Supplier<? extends T> supplier) {
        final CountDownLatch shutdown = new CountDownLatch(1);
        final ResultProvider<T> provider = new ResultProvider<>();
        final Thread thread = new Thread(() -> {
            provider.setResult(supplier.get());
            shutdown.countDown();
        });
        final CapturingUncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler(shutdown);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return new AsyncSupplier<>(shutdown, handler, provider);
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
        final boolean timedOut = !this.shutdown.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (timedOut) {
            throw new TimeoutException("Timeout awaiting result in " + timeout);
        }
        this.exceptionHandler.throwException();
        return this.resultProvider.getResult();
    }

    @RequiredArgsConstructor
    private static class CapturingUncaughtExceptionHandler implements UncaughtExceptionHandler {
        private final @NonNull CountDownLatch countDownLatch;
        private Throwable lastException;

        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            this.lastException = e;
            this.countDownLatch.countDown();
        }

        private void throwException() {
            if (this.lastException == null) {
                return;
            }
            if (this.lastException instanceof RuntimeException) {
                throw (RuntimeException) this.lastException;
            }
            throw new ExecutionException("Thread threw an exception", this.lastException);
        }
    }

    @Setter
    @Getter
    @NoArgsConstructor
    private static class ResultProvider<T> {
        private T result;
    }
}
