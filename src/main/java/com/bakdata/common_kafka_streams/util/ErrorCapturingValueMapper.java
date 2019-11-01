package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueMapper<V, VR> implements ValueMapper<V, ProcessedValue<V, VR>> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueMapper} and capture thrown exceptions.
     * <pre>{@code
     * final ValueMapper<V, VR> mapper = ...;
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
     * @param mapper {@code ValueMapper} whose exceptions should be captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, ProcessedValue<V, VR>> captureErrors(
            final ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorCapturingValueMapper<>(mapper);
    }

    @Override
    public ProcessedValue<V, VR> apply(final V value) {
        try {
            final VR newValue = this.wrapped.apply(value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }
}
