/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

package com.bakdata.kafka;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.Named;

@RequiredArgsConstructor
class ValueKErrorStream<K, V, VR> implements KErrorStream<K, V, K, VR> {
    private final @NonNull ImprovedKStream<K, ProcessedValue<V, VR>> stream;

    @Override
    public ImprovedKStream<K, VR> values() {
        return this.stream.flatMapValues(ProcessedValue::getValues);
    }

    @Override
    public ImprovedKStream<K, VR> values(final Named named) {
        return this.stream.flatMapValues(ProcessedValue::getValues, named);
    }

    @Override
    public ImprovedKStream<K, ProcessingError<V>> errors() {
        return this.stream.flatMapValues(ProcessedValue::getErrors);
    }

    @Override
    public ImprovedKStream<K, ProcessingError<V>> errors(final Named named) {
        return this.stream.flatMapValues(ProcessedValue::getErrors, named);
    }
}
