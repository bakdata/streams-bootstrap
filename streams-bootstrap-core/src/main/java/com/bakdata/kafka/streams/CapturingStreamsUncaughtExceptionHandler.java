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

package com.bakdata.kafka.streams;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@RequiredArgsConstructor
class CapturingStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    private final @NonNull StreamsUncaughtExceptionHandler wrapped;
    private Throwable lastException;

    @Override
    public StreamThreadExceptionResponse handle(final Throwable exception) {
        final StreamThreadExceptionResponse response = this.wrapped.handle(exception);
        this.lastException = exception;
        return response;
    }

    void throwException() {
        if (this.lastException instanceof final RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new StreamsApplicationException("Kafka Streams has transitioned to error", this.lastException);
    }
}
