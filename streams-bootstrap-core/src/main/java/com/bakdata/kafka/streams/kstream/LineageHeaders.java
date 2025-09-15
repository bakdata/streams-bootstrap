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

import java.nio.charset.StandardCharsets;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class LineageHeaders {
    private static final String LINEAGE_PREFIX = "lineage.";
    /**
     * Header indicating the topic the record was read from.
     */
    public static final String TOPIC_HEADER = LINEAGE_PREFIX + "topic";
    /**
     * Header indicating the partition the record was read from.
     */
    public static final String PARTITION_HEADER = LINEAGE_PREFIX + "partition";
    /**
     * Header indicating the offset the record was read from.
     */
    public static final String OFFSET_HEADER = LINEAGE_PREFIX + "offset";

    @Getter(AccessLevel.PACKAGE)
    @Accessors(fluent = true)
    private final @NonNull Headers headers;

    LineageHeaders addTopicHeader(final String topic) {
        if (topic == null) {
            return this;
        }
        return new LineageHeaders(this.headers.add(TOPIC_HEADER, topic.getBytes(StandardCharsets.UTF_8)));
    }

    LineageHeaders addPartitionHeader(final int partition) {
        if (partition < 0) {
            return this;
        }
        //TODO serialize more compact as int? But then UI tools usually can't handle it
        return new LineageHeaders(
                this.headers.add(PARTITION_HEADER, Integer.toString(partition).getBytes(StandardCharsets.UTF_8)));
    }

    LineageHeaders addOffsetHeader(final long offset) {
        if (offset < 0) {
            return this;
        }
        //TODO serialize more compact as long? But then UI tools usually can't handle it
        return new LineageHeaders(
                this.headers.add(OFFSET_HEADER, Long.toString(offset).getBytes(StandardCharsets.UTF_8)));
    }
}
