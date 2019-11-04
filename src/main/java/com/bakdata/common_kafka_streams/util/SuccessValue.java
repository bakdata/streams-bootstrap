package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class SuccessValue<V, VR> implements ProcessedValue<V, VR> {

    private final VR record;

    static <V, VR> ProcessedValue<V, VR> of(final VR vr) {
        return new SuccessValue<>(vr);
    }

    @Override
    public Iterable<ProcessingError<V>> getErrors() {
        return emptyList();
    }

    @Override
    public Iterable<VR> getValues() {
        // allow null values
        return Collections.singletonList(this.record);
    }

}
