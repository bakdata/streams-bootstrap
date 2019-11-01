package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;

    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> mapper) {
        return new ErrorCapturingValueTransformerWithKey<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public ProcessedValue<V, VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

}
