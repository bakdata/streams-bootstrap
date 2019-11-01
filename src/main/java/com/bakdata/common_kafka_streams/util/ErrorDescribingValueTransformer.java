package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;

@Slf4j
public final class ErrorDescribingValueTransformer<V, VR> extends DecoratorValueTransformer<V, VR> {

    private ErrorDescribingValueTransformer(final @NonNull ValueTransformer<V, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code ValueTransformer} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformer<V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> describeErrors(transformer));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> DecoratorValueTransformer<V, VR> describeErrors(final ValueTransformer<V, VR> transformer) {
        return new ErrorDescribingValueTransformer<>(transformer);
    }

    @Override
    public VR transform(final V value) {
        try {
            return super.transform(value);
        } catch (final Exception e) {
            throw new RuntimeException("Cannot process " + ErrorUtil.toString(value), e);
        }
    }

}
