package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<VR>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;

    public static <V, VR> ValueMapper<V, Iterable<VR>> logErrors(
            final ValueMapper<? super V, ? extends Iterable<VR>> mapper) {
        return new ErrorLoggingFlatValueMapper<>(mapper);
    }

    @Override
    public Iterable<VR> apply(final V value) {
        try {
            return this.wrapped.apply(value);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process " + ErrorUtil.toString(value), e);
            return emptyList();
        }
    }

}
