package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

@Slf4j
@RequiredArgsConstructor
public class ErrorDescribingValueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, VR> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueMapperWithKey} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapperWithKey<K, V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.mapValues(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     */
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
