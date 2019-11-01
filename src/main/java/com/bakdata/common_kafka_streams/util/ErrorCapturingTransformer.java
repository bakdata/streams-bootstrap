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
public class ErrorCapturingTransformer<K, V, KR, VR>
        implements Transformer<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> {
    private final @NonNull Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> wrapped;

    public static <K, V, KR, VR> Transformer<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> mapper) {
        return new ErrorCapturingTransformer<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(new ErrorCapturingProcessorContext(context));
    }

    @Override
    public KeyValue<KR, ProcessedKeyValue<K, V, VR>> transform(final K key, final V value) {
        try {
            final KeyValue<KR, VR> newKeyValue = this.wrapped.transform(key, value);
            if (newKeyValue == null) {
                return null;
            }
            final ProcessedKeyValue<K, V, VR> recordWithOldKey = SuccessKeyValue.of(newKeyValue.value);
            return KeyValue.pair(newKeyValue.key, recordWithOldKey);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(key, value, e);
            // new key is only relevant if no error occurs
            return KeyValue.pair(null, errorWithOldKey);
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

        private static <K, V, VR> ProcessedKeyValue<K, V, VR> getValue(final VR value) {
            return SuccessKeyValue.of(value);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, to);
        }

        @Override
        public <K, V> void forward(final K key, final V value) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, childIndex);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, childName);
        }
    }

}
