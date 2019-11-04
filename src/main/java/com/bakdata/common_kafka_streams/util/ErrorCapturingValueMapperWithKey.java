package com.bakdata.common_kafka_streams.util;

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * Wrap a {@code ValueMapperWithKey} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueMapperWithKey)
 * @see #captureErrors(ValueMapperWithKey, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingValueMapperWithKey<K, V, VR>
        implements ValueMapperWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueMapperWithKey} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @see #captureErrors(ValueMapperWithKey, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return captureErrors(mapper, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueMapperWithKey} and capture thrown exceptions.
     * <pre>{@code
     * final ValueMapperWithKey<K, V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.mapValues(captureErrors(mapper));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingValueMapperWithKey<>(mapper, errorFilter);
    }

    @Override
    public ProcessedValue<V, VR> apply(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.apply(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }
}
