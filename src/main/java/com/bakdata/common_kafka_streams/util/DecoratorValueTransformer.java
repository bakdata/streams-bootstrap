package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@RequiredArgsConstructor
public abstract class DecoratorValueTransformer<V, R> implements ValueTransformer<V, R> {
    private final @NonNull ValueTransformer<V, R> wrapped;

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public R transform(final V value) {
        return this.wrapped.transform(value);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }
}
