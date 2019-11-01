package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingValueMapper<V, VR> implements ValueMapper<V, VR> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapper<V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.mapValues(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapper} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, VR> describeErrors(final ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorDescribingValueMapper<>(mapper);
    }

    @Override
    public VR apply(final V v) {
        try {
            return this.wrapped.apply(v);
        } catch (final Exception e) {
            throw new RuntimeException("Cannot process " + ErrorUtil.toString(v), e);
        }
    }
}
