package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueMapper} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapper<V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.mapValues(logErrors(mapper));
     * }
     * </pre>
     *
     * Recoverable Kafka exceptions such as a schema registry timeout are forwarded and not captured. See {@link
     * ErrorUtil#shouldForwardError(Exception)}
     *
     * @param mapper {@code ValueMapper} whose exceptions should be logged
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, Iterable<VR>> logErrors(final ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorLoggingValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final V value) {
        try {
            final VR newValue = this.wrapped.apply(value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process " + ErrorUtil.toString(value), e);
            return emptyList();
        }
    }
}
