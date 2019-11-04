package com.bakdata.common_kafka_streams.util;

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Wrap a {@code ValueTransformer} and capture thrown exceptions.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueTransformer)
 * @see #captureErrors(ValueTransformer, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingValueTransformer<V, VR> implements ValueTransformer<V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformer<? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformer} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @see #captureErrors(ValueTransformer, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <V, VR> ValueTransformer<V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformer<? super V, ? extends VR> transformer) {
        return captureErrors(transformer, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueTransformer} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerSupplier<V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.transformValues(() -> captureErrors(transformer.get()));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> ValueTransformer<V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformer<? super V, ? extends VR> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingValueTransformer<>(transformer, errorFilter);
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
    public ProcessedValue<V, VR> transform(final V value) {
        try {
            final VR newValue = this.wrapped.transform(value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }

}
