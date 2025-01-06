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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;

@Value
@Getter(AccessLevel.PACKAGE)
public class StreamsContext {

    @NonNull
    StreamsTopicConfig topics;
    @NonNull
    Configurator configurator;

    public <KR, VR> ImprovedKStream<KR, VR> newStream(final KStream<KR, VR> stream) {
        return new ImprovedKStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedKGroupedStream<KR, VR> newGroupedStream(final KGroupedStream<KR, VR> stream) {
        return new ImprovedKGroupedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedTimeWindowedKStream<KR, VR> newTimeWindowedStream(
            final TimeWindowedKStream<KR, VR> stream) {
        return new ImprovedTimeWindowedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedSessionWindowedKStream<KR, VR> newSessionWindowedStream(
            final SessionWindowedKStream<KR, VR> stream) {
        return new ImprovedSessionWindowedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedTimeWindowedCogroupedKStream<KR, VR> newTimeWindowedCogroupedStream(
            final TimeWindowedCogroupedKStream<KR, VR> stream) {
        return new ImprovedTimeWindowedCogroupedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedSessionWindowedCogroupedKStream<KR, VR> newSessionWindowedCogroupedStream(
            final SessionWindowedCogroupedKStream<KR, VR> stream) {
        return new ImprovedSessionWindowedCogroupedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedCogroupedKStream<KR, VR> newCogroupedStream(final CogroupedKStream<KR, VR> stream) {
        return new ImprovedCogroupedStreamImpl<>(stream, this);
    }

    public <KR, VR> ImprovedKTable<KR, VR> newTable(final KTable<KR, VR> table) {
        return new ImprovedKTableImpl<>(table, this);
    }

    public <KR, VR> ImprovedKGroupedTable<KR, VR> newGroupedTable(final KGroupedTable<KR, VR> table) {
        return new ImprovedKGroupedTableImpl<>(table, this);
    }
}
