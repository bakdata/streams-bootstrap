/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

package com.bakdata.common_kafka_streams.util;

/**
 * A processed value is created upon capturing errors in a streams topology. It can either contain a successfully
 * processed value or a {@link ProcessingError} describing the input value and the {@link Exception} that has been
 * thrown.
 *
 * @param <V> the type of the old value before applying the error capturer
 * @param <VR> the type of the new value after applying the error capturer
 */
public interface ProcessedValue<V, VR> {

    /**
     * Extract errors from a processed value. If an error is available, it will give information about the input value
     * and the {@link Exception} that was thrown while attempting to map it to a new value.
     *
     * @return A single {@link ProcessingError} if an exception was thrown upon processing the input value or an empty
     * list
     */
    Iterable<ProcessingError<V>> getErrors();

    /**
     * Extract successfully processed values from a processed value.
     *
     * @return A single value if processing was successful or an empty list
     */
    Iterable<VR> getValues();
}
