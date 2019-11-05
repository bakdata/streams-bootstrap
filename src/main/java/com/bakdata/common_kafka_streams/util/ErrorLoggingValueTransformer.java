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
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Wrap a {@code ValueTransformer} and log thrown exceptions with input key and value.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(ValueTransformer)
 * @see #logErrors(ValueTransformer, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingValueTransformer<V, VR> implements ValueTransformer<V, Iterable<VR>> {
    private final @NonNull ValueTransformer<? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformer} and log thrown exceptions with input key and value. Recoverable Kafka exceptions
     * such as a schema registry timeout are forwarded and not captured.
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be logged
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     * @see #logErrors(ValueTransformer, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <V, VR> ValueTransformer<V, Iterable<VR>> logErrors(
            final ValueTransformer<? super V, ? extends VR> transformer) {
        return logErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformer} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerSupplier<V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> logErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> ValueTransformer<V, Iterable<VR>> logErrors(
            final ValueTransformer<? super V, ? extends VR> transformer, final Predicate<Exception> errorFilter) {
        return new ErrorLoggingValueTransformer<>(transformer, errorFilter);
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
    public Iterable<VR> transform(final V value) {
        try {
            final VR newValue = this.wrapped.transform(value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process " + ErrorUtil.toString(value), e);
            return emptyList();
        }
    }

}
