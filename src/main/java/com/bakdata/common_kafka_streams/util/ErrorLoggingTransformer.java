package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingTransformer<K, V, KR, VR> implements Transformer<K, V, KeyValue<KR, VR>> {
    private final @NonNull Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> wrapped;

    public static <K, V, KR, VR> Transformer<K, V, KeyValue<KR, VR>> logErrors(
            final Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> mapper) {
        return new ErrorLoggingTransformer<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(new ErrorCapturingProcessorContext(context));
    }

    @Override
    public KeyValue<KR, VR> transform(final K key, final V value) {
        try {
            return this.wrapped.transform(key, value);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return null;
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    private static final class ErrorCapturingProcessorContext extends DecoratorProcessorContext {
        private ErrorCapturingProcessorContext(final @NonNull ProcessorContext wrapped) {
            super(wrapped);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {
            super.forward(key, value, to);
        }

        @Override
        public <K, V> void forward(final K key, final V value) {
            super.forward(key, value);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {
            super.forward(key, value, childIndex);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {
            super.forward(key, value, childName);
        }
    }

}
