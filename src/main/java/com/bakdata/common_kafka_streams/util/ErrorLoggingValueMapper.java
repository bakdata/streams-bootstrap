package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    public static <V, VR> ValueMapper<V, Iterable<VR>> logErrors(final ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorLoggingValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final V value) {
        try {
            final VR newValue = this.wrapped.apply(value);
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
}
