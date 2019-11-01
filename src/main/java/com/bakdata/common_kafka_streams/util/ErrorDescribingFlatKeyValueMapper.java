package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingFlatKeyValueMapper<K, V, VR> implements KeyValueMapper<K, V, Iterable<VR>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends Iterable<VR>> wrapped;

    public static <K, V, R> KeyValueMapper<K, V, Iterable<R>> describeErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<R>> mapper) {
        return new ErrorDescribingFlatKeyValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }
}
