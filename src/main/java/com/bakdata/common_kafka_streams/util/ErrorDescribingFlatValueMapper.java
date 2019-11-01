package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueMapper;

@RequiredArgsConstructor
public class ErrorDescribingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;

    /**
     * Wrap a {@code ValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapper<V, Iterable<VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.flatMapValues(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapper} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, Iterable<VR>> describeErrors(final ValueMapper<? super V, ? extends Iterable<VR>> mapper) {
        return new ErrorDescribingFlatValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final V v) {
        try {
            return this.wrapped.apply(v);
        } catch (final Exception e) {
            throw new RuntimeException("Cannot process " + ErrorUtil.toString(v), e);
        }
    }
}