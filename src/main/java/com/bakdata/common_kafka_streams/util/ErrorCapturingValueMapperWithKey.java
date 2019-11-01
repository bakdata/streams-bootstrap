package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends VR> wrapped;

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
     * Recoverable Kafka exceptions such as a schema registry timeout are forwarded and not captured. See {@link
     * ErrorUtil#shouldForwardError(Exception)}
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return new ErrorCapturingValueMapperWithKey<>(mapper);
    }

    @Override
    public ProcessedValue<V, VR> apply(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.apply(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }
}
