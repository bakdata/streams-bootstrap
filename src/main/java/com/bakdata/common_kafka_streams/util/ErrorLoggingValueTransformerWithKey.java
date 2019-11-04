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

import static java.util.Collections.emptyList;

import java.util.Collections;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(ValueTransformerWithKey)
 * @see #logErrors(ValueTransformerWithKey, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @see #logErrors(ValueTransformerWithKey, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer) {
        return logErrors(transformer, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> logErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorLoggingValueTransformerWithKey<>(transformer, errorFilter);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public Iterable<VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return emptyList();
        }
    }

}
