package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueMapper<V, VR> implements ValueMapper<V, ProcessedValue<V, VR>> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    public static <V, VR> ValueMapper<V, ProcessedValue<V, VR>> captureErrors(
            final ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorCapturingValueMapper<>(mapper);
    }

    @Override
    public ProcessedValue<V, VR> apply(final V value) {
        try {
            final VR newValue = this.wrapped.apply(value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }
}
