package com.bakdata.common_kafka_streams.util;

import static org.jooq.lambda.Seq.seq;

import java.util.List;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<ProcessedValue<V, VR>>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;

    public static <V, VR> ValueMapper<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueMapper<? super V, ? extends Iterable<VR>> mapper) {
        return new ErrorCapturingFlatValueMapper<>(mapper);
    }

    @Override
    public Iterable<ProcessedValue<V, VR>> apply(final V value) {
        try {
            final Iterable<VR> newValues = this.wrapped.apply(value);
            return seq(newValues).map(SuccessValue::of);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return List.of(ErrorValue.of(value, e));
        }
    }

}
