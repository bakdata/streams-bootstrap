package com.bakdata.common_kafka_streams.util;

import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.exception.ExceptionUtils;


/**
 * This class represents an error that has been thrown upon processing an input value. Both the input value and the
 * thrown exception are available for further error handling.
 *
 * @param <V> type of input value
 */
@Value
@Builder
public class ProcessingError<V> {

    private final V value;
    private final @NonNull Throwable throwable;

    /**
     * Create a default {@code DeadLetter} from this processing error. Usually, these dead letters are sent to a
     * dedicated error topic.
     *
     * @param description description of the context in which an exception has been thrown
     * @return {@code DeadLetter}
     */
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
