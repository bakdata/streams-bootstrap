package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;

    /**
     * Wrap a {@code ValueMapper} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapper<V, Iterable<VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.flatMapValues(logErrors(mapper));
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
    public static <V, VR> ValueMapper<V, Iterable<VR>> logErrors(
            final ValueMapper<? super V, ? extends Iterable<VR>> mapper) {
        return new ErrorLoggingFlatValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final V value) {
        try {
            return this.wrapped.apply(value);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process " + ErrorUtil.toString(value), e);
            return emptyList();
        }
    }

}
