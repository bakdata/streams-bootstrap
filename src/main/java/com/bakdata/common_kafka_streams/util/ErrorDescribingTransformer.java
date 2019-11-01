package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Transformer;

@Slf4j
public final class ErrorDescribingTransformer<K, V, VR> extends DecoratorTransformer<K, V, VR> {

    private ErrorDescribingTransformer(final @NonNull Transformer<K, V, VR> wrapped) {
        super(wrapped);
    }

    public static <K, V, R> DecoratorTransformer<K, V, R> describeErrors(final Transformer<K, V, R> transformer) {
        return new ErrorDescribingTransformer<>(transformer);
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
