package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingKeyValueMapper<K, V, VR> implements KeyValueMapper<K, V, VR> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code KeyValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.map(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of map result
     * @return {@code KeyValueMapper}
     */
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
