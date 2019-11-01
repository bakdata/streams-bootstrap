package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingValueMapper<V, VR> implements ValueMapper<V, VR> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    public static <T, R> ValueMapper<T, R> describeErrors(final ValueMapper<? super T, ? extends R> mapper) {
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
