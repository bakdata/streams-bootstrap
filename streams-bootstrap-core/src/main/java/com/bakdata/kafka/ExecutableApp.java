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

/**
 * An application with a corresponding topic and Kafka configuration
 * @param <R> type returned by {@link #createRunner()} and {@link #createRunner(Object)}
 * @param <C> type returned by {@link #createCleanUpRunner()}
 * @param <O> type of options to create runner
 */
public interface ExecutableApp<R, C, O> extends AutoCloseable {

    @Override
    void close();

    /**
     * Create {@link Runner} in order to run application with default options
     * @return {@link Runner}
     */
    R createRunner();

    /**
     * Create {@link Runner} in order to run application
     * @param options options for creating runner
     * @return {@link Runner}
     */
    R createRunner(O options);

    /**
     * Create {@link CleanUpRunner} in order to clean application
     * @return {@link CleanUpRunner}
     */
    C createCleanUpRunner();
}
