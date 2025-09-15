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

package com.bakdata.kafka.streams.kstream;

import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

class LineageProcessor<K, V> implements FixedKeyProcessor<K, V, V> {
    private FixedKeyProcessorContext<K, V> context;

    private static Headers addHeaders(final Headers headers, final RecordMetadata metadata) {
        return new LineageHeaders(new RecordHeaders(headers))
                .addTopicHeader(metadata.topic())
                .addPartitionHeader(metadata.partition())
                .addOffsetHeader(metadata.offset())
                .headers();
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, V> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<K, V> rekord) {
        final Optional<RecordMetadata> metadata = this.context.recordMetadata();
        final Headers headers = rekord.headers();
        final Headers newHeaders = metadata.map(m -> addHeaders(headers, m))
                .orElse(headers);
        this.context.forward(rekord.withHeaders(newHeaders));
    }
}
