/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.common_kafka_streams.util;

import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;


/**
 * This class represents an error that has been thrown upon processing an input value. Both the input value and the
 * thrown exception are available for further error handling.
 *
 * @param <V> type of input value
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ProcessingError<V> {

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
