package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Base class for decorating a {@code Transformer}
 */
@RequiredArgsConstructor
public abstract class DecoratorTransformer<K, V, R> implements Transformer<K, V, R> {
    private final @NonNull Transformer<K, V, R> wrapped;

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public R transform(final K key, final V value) {
        return this.wrapped.transform(key, value);
    }
}
