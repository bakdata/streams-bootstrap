package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Transformer;

/**
 * Wrap a {@code Transformer} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of transformation result
 * @see #describeErrors(Transformer)
 */
public final class ErrorDescribingTransformer<K, V, R> extends DecoratorTransformer<K, V, R> {

    private ErrorDescribingTransformer(final @NonNull Transformer<K, V, R> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code Transformer} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final TransformerSupplier<K, V, KeyValue<KR, VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(() -> describeErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code Transformer}
     */
    public static <K, V, R> DecoratorTransformer<K, V, R> describeErrors(final Transformer<K, V, R> transformer) {
        return new ErrorDescribingTransformer<>(transformer);
    }

    @Override
    public R transform(final K key, final V value) {
        try {
            return super.transform(key, value);
        } catch (final Exception e) {
            throw new RuntimeException(
                    "Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
        }
    }

}
