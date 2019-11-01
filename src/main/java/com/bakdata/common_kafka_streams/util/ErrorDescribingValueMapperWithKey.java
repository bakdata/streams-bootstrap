package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingValueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, VR> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends VR> wrapped;

    public static <K, V, VR> ValueMapperWithKey<K, V, VR> describeErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return new ErrorDescribingValueMapperWithKey<>(mapper);
    }

    @Override
    public VR apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }
}
