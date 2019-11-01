package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingKeyValueMapper<K, V, VR> implements KeyValueMapper<K, V, VR> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends VR> wrapped;

    public static <K, V, R> KeyValueMapper<K, V, R> describeErrors(
            final KeyValueMapper<? super K, ? super V, ? extends R> mapper) {
        return new ErrorDescribingKeyValueMapper<>(mapper);
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
