package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@RequiredArgsConstructor
public abstract class DecoratorValueTransformerWithKey<K, V, R> implements ValueTransformerWithKey<K, V, R> {
    private final @NonNull ValueTransformerWithKey<K, V, R> wrapped;

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public R transform(final K key, final V value) {
        return this.wrapped.transform(key, value);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }
}
