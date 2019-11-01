package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingValueTransformer<V, VR> implements ValueTransformer<V, Iterable<VR>> {
    private final @NonNull ValueTransformer<? super V, ? extends VR> wrapped;

    public static <V, VR> ValueTransformer<V, Iterable<VR>> logErrors(
            final ValueTransformer<? super V, ? extends VR> mapper) {
        return new ErrorLoggingValueTransformer<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public Iterable<VR> transform(final V value) {
        try {
            final VR newValue = this.wrapped.transform(value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process " + ErrorUtil.toString(value), e);
            return emptyList();
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

}
