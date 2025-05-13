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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class AsyncRunnableTest {

    @Test
    void shouldRun() {
        final AsyncRunnable<Void> runnable = AsyncRunnable.runAsync(() -> {});
        final Duration timeout = Duration.ofSeconds(1L);
        assertThatCode(() -> runnable.await(timeout)).doesNotThrowAnyException();
    }

    @Test
    void shouldProvideResult() {
        final AsyncRunnable<Integer> runnable = AsyncRunnable.runAsync(() -> 1);
        final Duration timeout = Duration.ofSeconds(1L);
        assertThat(runnable.await(timeout)).isEqualTo(1L);
    }

    @Test
    void shouldThrowException() {
        final RuntimeException exception = new RuntimeException("Test exception");
        final AsyncRunnable<Void> runnable = AsyncRunnable.runAsync(() -> {
            throw exception;
        });
        final Duration timeout = Duration.ofSeconds(1L);
        assertThatThrownBy(() -> runnable.await(timeout)).isEqualTo(exception);
    }

    @Test
    void shouldThrowOnTimeout() {
        final AsyncRunnable<Void> runnable = AsyncRunnable.runAsync(() -> {
            try {
                Thread.sleep(Duration.ofSeconds(2L).toMillis());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        final Duration timeout = Duration.ofSeconds(1L);
        assertThatThrownBy(() -> runnable.await(timeout))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Timeout awaiting runnable");
    }
}
