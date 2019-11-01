package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueMapper;

@RequiredArgsConstructor
public class ErrorDescribingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;

    public static <T, R> ValueMapper<T, Iterable<R>> describeErrors(final ValueMapper<? super T, ? extends Iterable<R>> mapper) {
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