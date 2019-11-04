package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.streams.KeyValue;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class SuccessKeyValue<K, V, VR> implements ProcessedKeyValue<K, V, VR> {

    private final VR record;

    static <K, V, VR> ProcessedKeyValue<K, V, VR> of(final VR vr) {
        return new SuccessKeyValue<>(vr);
    }

    @Override
    public Iterable<KeyValue<K, ProcessingError<V>>> getErrors() {
        return emptyList();
    }

    @Override
    public Iterable<VR> getValues() {
        // allow null values
        return Collections.singletonList(this.record);
    }
}
