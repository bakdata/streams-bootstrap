package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;

/**
 * Wrap a {@code ValueTransformerWithKey} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #describeErrors(ValueTransformerWithKey)
 */
public final class ErrorDescribingValueTransformerWithKey<K, V, VR> extends DecoratorValueTransformerWithKey<K, V, VR> {

    private ErrorDescribingValueTransformerWithKey(final @NonNull ValueTransformerWithKey<K, V, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> describeErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> DecoratorValueTransformerWithKey<K, V, VR> describeErrors(
            final ValueTransformerWithKey<K, V, VR> transformer) {
        return new ErrorDescribingValueTransformerWithKey<>(transformer);
    }

    @Override
    public VR transform(final K key, final V value) {
        try {
            return super.transform(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }

}
