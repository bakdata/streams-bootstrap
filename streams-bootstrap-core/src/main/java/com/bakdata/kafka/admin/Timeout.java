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

package com.bakdata.kafka.admin;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.KafkaFuture;

@RequiredArgsConstructor
class Timeout {
    private final @NonNull Duration duration;

    <T> T get(final KafkaFuture<T> future, final Supplier<String> messageSupplier) {
        try {
            return future.get(this.duration.toSeconds(), TimeUnit.SECONDS);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof final RuntimeException cause) {
                throw cause;
            }
            throw new KafkaAdminException(messageSupplier.get(), e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException(messageSupplier.get(), e);
        } catch (final TimeoutException e) {
            throw new KafkaAdminException(messageSupplier.get(), e);
        }
    }
}
