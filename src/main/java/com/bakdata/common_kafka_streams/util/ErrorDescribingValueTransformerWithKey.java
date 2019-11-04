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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;

/**
 * Wrap a {@code ValueTransformerWithKey} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #describeErrors(ValueTransformerWithKey)
 */
public final class ErrorDescribingValueTransformerWithKey<K, V, VR> extends DecoratorValueTransformerWithKey<K, V, VR> {

    private ErrorDescribingValueTransformerWithKey(final @NonNull ValueTransformerWithKey<K, V, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> describeErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> DecoratorValueTransformerWithKey<K, V, VR> describeErrors(
            final ValueTransformerWithKey<K, V, VR> transformer) {
        return new ErrorDescribingValueTransformerWithKey<>(transformer);
    }

    @Override
    public VR transform(final K key, final V value) {
        try {
            return super.transform(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }

}
