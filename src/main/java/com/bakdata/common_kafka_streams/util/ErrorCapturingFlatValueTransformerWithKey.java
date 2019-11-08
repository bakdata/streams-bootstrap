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

import java.util.List;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.jooq.lambda.Seq;

/**
 * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueTransformerWithKey)
 * @see #captureErrors(ValueTransformerWithKey, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingFlatValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, Iterable<ProcessedValue<V, VR>>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions. Recoverable Kafka exceptions such as a
     * schema registry timeout are forwarded and not captured.
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     * @see #captureErrors(ValueTransformerWithKey, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> transformer) {
        return captureErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, Iterable<VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.flatTransformValues(() -> captureErrors(transformer.get()));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingFlatValueTransformerWithKey<>(transformer, errorFilter);
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
    public Iterable<ProcessedValue<V, VR>> transform(final K key, final V value) {
        try {
            final Iterable<VR> newValues = this.wrapped.transform(key, value);
            return Seq.seq(newValues).map(SuccessValue::of);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return List.of(ErrorValue.of(value, e));
        }
    }

}
