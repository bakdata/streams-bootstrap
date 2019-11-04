package com.bakdata.common_kafka_streams.util;

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

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
public final class ErrorCapturingValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions. Recoverable Kafka exceptions such as a
     * schema registry timeout are forwarded and not captured.
     *
     * @see #captureErrors(ValueTransformerWithKey, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer) {
        return captureErrors(transformer, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.transformValues(() -> captureErrors(transformer.get()));
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
    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingValueTransformerWithKey<>(transformer, errorFilter);
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
    public ProcessedValue<V, VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }

}
