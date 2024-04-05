/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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
 * @param <T> type of {@link Runner}
 * @param <C> type of {@link CleanUpRunner}
 * @param <O> type of options to create {@link Runner}
 */
public interface ExecutableApp<T extends Runner, C extends CleanUpRunner, O> extends AutoCloseable {

    @Override
    void close();

    /**
     * Create {@code Runner} in order to run application with default options
     * @return {@code Runner}
     */
    T createRunner();

    /**
     * Create {@code Runner} in order to run application
     * @param options options for creating runner
     * @return {@code Runner}
     */
    T createRunner(O options);

    /**
     * Create {@code CleanUpRunner} in order to clean application
     * @return {@code CleanUpRunner}
     */
    C createCleanUpRunner();
}