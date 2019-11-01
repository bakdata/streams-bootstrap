package com.bakdata.common_kafka_streams.util;

import java.util.List;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.jooq.lambda.Seq;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingFlatValueMapperWithKey<K, V, VR>
        implements ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends Iterable<VR>> wrapped;

    public static <K, V, VR> ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<VR>> mapper) {
        return new ErrorCapturingFlatValueMapperWithKey<>(mapper);
    }

    @Override
    public Iterable<ProcessedValue<V, VR>> apply(final K key, final V value) {
        try {
            final Iterable<VR> newValues = this.wrapped.apply(key, value);
            return Seq.seq(newValues).map(SuccessValue::of);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return List.of(ErrorValue.of(value, e));
        }
    }
}

