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

import static java.util.Collections.emptyList;

import java.util.List;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class ErrorKeyValue<K, V, VR> implements ProcessedKeyValue<K, V, VR> {
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
