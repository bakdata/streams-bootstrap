package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.List;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.streams.KeyValue;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class ErrorKeyValue<K, V, VR> implements ProcessedKeyValue<K, V, VR> {
    private final K oldKey;
    private final @NonNull ProcessingError<V> error;

    static <K, V, VR> ProcessedKeyValue<K, V, VR> of(final K oldKey, final V value,
            final Throwable throwable) {
        return new ErrorKeyValue<>(oldKey, ProcessingError.<V>builder()
                .throwable(throwable)
                .value(value)
                .build());
    }

    @Override
    public Iterable<KeyValue<K, ProcessingError<V>>> getErrors() {
        return List.of(KeyValue.pair(this.oldKey, this.error));
    }

    @Override
    public Iterable<VR> getValues() {
        return emptyList();
    }
}
