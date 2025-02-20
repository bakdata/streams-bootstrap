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
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AsyncRunnable {
    private final @NonNull CountDownLatch countDownLatch;
    private final @NonNull CapturingUncaughtExceptionHandler handler;

    public static AsyncRunnable runAsync(final Runnable app) {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Thread thread = new Thread(() -> {
            app.run();
            countDownLatch.countDown();
        });
        final CapturingUncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler(countDownLatch);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return new AsyncRunnable(countDownLatch, handler);
    }

    public void await(final Duration timeout) {
        try {
            final boolean timedOut = !this.countDownLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (timedOut) {
                throw new RuntimeException("Timeout awaiting application shutdown");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error awaiting application shutdown", e);
        }
        this.handler.throwException();
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
            throw new RuntimeException("Thread threw an exception", this.lastException);
        }
    }
}
