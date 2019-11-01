package com.bakdata.common_kafka_streams.util;

import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Value
@Builder
public class ProcessingError<V> {

    private final V value;
    private final @NonNull Throwable throwable;

    public DeadLetter createDeadLetter(final @NonNull String description) {
        return DeadLetter.newBuilder()
                .setInputValue(Optional.ofNullable(this.value).map(ErrorUtil::toString).orElse(null))
                .setCause(ErrorDescription.newBuilder()
                        .setMessage(this.throwable.getMessage())
                        .setStackTrace(ExceptionUtils.getStackTrace(this.throwable))
                        .build())
                .setDescription(description)
                .build();
    }
}
