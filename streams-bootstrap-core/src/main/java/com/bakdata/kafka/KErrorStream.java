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

import org.apache.kafka.streams.kstream.Named;

/**
 * {@link KStreamX} that contains successfully processed records and errors of a previous operation
 * @param <K> type of keys in the original {@link KStreamX}
 * @param <V> type of values in the original {@link KStreamX}
 * @param <KR> type of keys in the processed {@link KStreamX}
 * @param <VR> type of values in the processed {@link KStreamX}
 */
public interface KErrorStream<K, V, KR, VR> {

    /**
     * Get the stream of successfully processed values
     * @return stream of processed values
     */
    KStreamX<KR, VR> values();

    /**
     * Get the stream of successfully processed values
     * @param named name of the processor
     * @return stream of processed values
     */
    KStreamX<KR, VR> values(Named named);

    /**
     * Get the stream of errors that occurred during processing
     * @return stream of errors
     */
    KStreamX<K, ProcessingError<V>> errors();

    /**
     * Get the stream of errors that occurred during processing
     * @param named name of the processor
     * @return stream of errors
     */
    KStreamX<K, ProcessingError<V>> errors(Named named);
}
