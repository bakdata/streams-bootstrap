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

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * Wrap a {@code KeyValueMapper} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of map result
 * @see #describeErrors(KeyValueMapper)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorDescribingFlatKeyValueMapper<K, V, R> implements KeyValueMapper<K, V, Iterable<R>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends Iterable<R>> wrapped;

    /**
     * Wrap a {@code KeyValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KeyValueMapper<K, V, Iterable<KeyValue<KR, VR>>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.flatMap(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of map result
     * @return {@code KeyValueMapper}
     */
    public static <K, V, R> KeyValueMapper<K, V, Iterable<R>> describeErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<R>> mapper) {
        return new ErrorDescribingFlatKeyValueMapper<>(mapper);
    }

    @Override
    public Iterable<R> apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            throw new ProcessingException(key, value, e);
        }
    }
}
