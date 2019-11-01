package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.List;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class ErrorValue<V, VR> implements ProcessedValue<V, VR> {
    private final @NonNull ProcessingError<V> error;

    static <V, VR> ProcessedValue<V, VR> of(final V value, final Throwable throwable) {
        return new ErrorValue<>(ProcessingError.<V>builder()
                .throwable(throwable)
                .value(value)
                .build());
    }

    @Override
    public Iterable<ProcessingError<V>> getErrors() {
        return List.of(this.error);
    }

    @Override
    public Iterable<VR> getValues() {
        return emptyList();
    }

}
