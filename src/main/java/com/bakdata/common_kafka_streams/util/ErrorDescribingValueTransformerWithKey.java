package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;

@Slf4j
public final class ErrorDescribingValueTransformerWithKey<K, V, VR> extends DecoratorValueTransformerWithKey<K, V, VR> {

    private ErrorDescribingValueTransformerWithKey(final @NonNull ValueTransformerWithKey<K, V, VR> wrapped) {
        super(wrapped);
    }

    public static <K, V, R> DecoratorValueTransformerWithKey<K, V, R> describeErrors(
            final ValueTransformerWithKey<K, V, R> transformer) {
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
