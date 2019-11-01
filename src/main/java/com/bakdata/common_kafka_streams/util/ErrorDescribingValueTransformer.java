package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;

@Slf4j
public final class ErrorDescribingValueTransformer<V, VR> extends DecoratorValueTransformer<V, VR> {

    private ErrorDescribingValueTransformer(final @NonNull ValueTransformer<V, VR> wrapped) {
        super(wrapped);
    }

    public static <V, R> DecoratorValueTransformer<V, R> describeErrors(final ValueTransformer<V, R> transformer) {
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
