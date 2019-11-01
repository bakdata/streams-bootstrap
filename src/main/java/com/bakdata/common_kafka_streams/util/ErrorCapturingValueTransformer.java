package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueTransformer<V, VR> implements ValueTransformer<V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformer<? super V, ? extends VR> wrapped;

    public static <V, VR> ValueTransformer<V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformer<? super V, ? extends VR> mapper) {
        return new ErrorCapturingValueTransformer<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public ProcessedValue<V, VR> transform(final V value) {
        try {
            final VR newValue = this.wrapped.transform(value);
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
