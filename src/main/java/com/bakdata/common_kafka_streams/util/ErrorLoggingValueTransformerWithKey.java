package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingValueTransformerWithKey<K, V, VR> implements ValueTransformerWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;

    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> mapper) {
        return new ErrorLoggingValueTransformerWithKey<>(mapper);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public Iterable<VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return emptyList();
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

}
