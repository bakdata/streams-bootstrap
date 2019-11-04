package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * Wrap a {@code KeyValueMapper} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of map result
 * @see #describeErrors(KeyValueMapper)
 */
@RequiredArgsConstructor
public class ErrorDescribingFlatKeyValueMapper<K, V, R> implements KeyValueMapper<K, V, Iterable<R>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends Iterable<R>> wrapped;

    /**
     * Wrap a {@code KeyValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KeyValueMapper<K, V, Iterable<KeyValue<KR, VR>>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.flatMap(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of map result
     * @return {@code KeyValueMapper}
     */
    public static <K, V, R> KeyValueMapper<K, V, Iterable<R>> describeErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<R>> mapper) {
        return new ErrorDescribingFlatKeyValueMapper<>(mapper);
    }

    @Override
    public Iterable<R> apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }
}
