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
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;

/**
 * Provides context for the configured Kafka Streams application environment, i.e., topic configuration and
 * StreamsConfig
 */
@Value
@Getter(AccessLevel.PACKAGE)
public class StreamsContext {

    @NonNull
    StreamsTopicConfig topics;
    @NonNull
    Configurator configurator;

    static <K, V> KStream<K, V> maybeUnwrap(final KStream<K, V> stream) {
        if (stream instanceof ImprovedKStreamImpl) {
            // Kafka Streams internally casts KStream to KStreamImpl in some cases
            return ((ImprovedKStreamImpl<K, V>) stream).getWrapped();
        }
        return stream;
    }

    static <K, V> KGroupedStream<K, V> maybeUnwrap(final KGroupedStream<K, V> stream) {
        if (stream instanceof ImprovedKGroupedStream) {
            // Kafka Streams internally casts KGroupedStream to KGroupedStreamImpl in some cases
            return ((ImprovedKGroupedStreamImpl<K, V>) stream).getWrapped();
        }
        return stream;
    }

    static <K, V> KTable<K, V> maybeUnwrap(final KTable<K, V> table) {
        if (table instanceof ImprovedKTableImpl) {
            // Kafka Streams internally casts KTable to KTableImpl in some cases
            return ((ImprovedKTableImpl<K, V>) table).getWrapped();
        }
        return table;
    }

    /**
     * Wrap a {@code KStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedKStream<KR, VR> wrap(final KStream<KR, VR> stream) {
        if (stream instanceof ImprovedKStream) {
            return (ImprovedKStream<KR, VR>) stream;
        }
        return new ImprovedKStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code KGroupedStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedKGroupedStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedKGroupedStream<KR, VR> wrap(final KGroupedStream<KR, VR> stream) {
        if (stream instanceof ImprovedKGroupedStream) {
            return (ImprovedKGroupedStream<KR, VR>) stream;
        }
        return new ImprovedKGroupedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code TimeWindowedKStream} and add methods to simplify Serde configuration, error handling, and topic
     * access
     * @param stream stream to be wrapped
     * @return {@code ImprovedTimeWindowedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedTimeWindowedKStream<KR, VR> wrap(
            final TimeWindowedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedTimeWindowedKStream) {
            return (ImprovedTimeWindowedKStream<KR, VR>) stream;
        }
        return new ImprovedTimeWindowedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code SessionWindowedKStream} and add methods to simplify Serde configuration, error handling, and
     * topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedSessionWindowedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedSessionWindowedKStream<KR, VR> wrap(
            final SessionWindowedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedSessionWindowedKStream) {
            return (ImprovedSessionWindowedKStream<KR, VR>) stream;
        }
        return new ImprovedSessionWindowedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code TimeWindowedCogroupedKStream} and add methods to simplify Serde configuration, error handling,
     * and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedTimeWindowedCogroupedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedTimeWindowedCogroupedKStream<KR, VR> wrap(
            final TimeWindowedCogroupedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedTimeWindowedCogroupedKStream) {
            return (ImprovedTimeWindowedCogroupedKStream<KR, VR>) stream;
        }
        return new ImprovedTimeWindowedCogroupedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code SessionWindowedCogroupedKStream} and add methods to simplify Serde configuration, error
     * handling, and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedSessionWindowedCogroupedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedSessionWindowedCogroupedKStream<KR, VR> wrap(
            final SessionWindowedCogroupedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedSessionWindowedCogroupedKStream) {
            return (ImprovedSessionWindowedCogroupedKStream<KR, VR>) stream;
        }
        return new ImprovedSessionWindowedCogroupedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code CogroupedKStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedCogroupedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedCogroupedKStream<KR, VR> wrap(final CogroupedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedCogroupedKStream) {
            return (ImprovedCogroupedKStream<KR, VR>) stream;
        }
        return new ImprovedCogroupedStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code BranchedKStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@code ImprovedBranchedKStream}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> ImprovedBranchedKStream<KR, VR> wrap(final BranchedKStream<KR, VR> stream) {
        if (stream instanceof ImprovedBranchedKStream) {
            return (ImprovedBranchedKStream<KR, VR>) stream;
        }
        return new ImprovedBranchedKStreamImpl<>(stream, this);
    }

    /**
     * Wrap a {@code KTable} and add methods to simplify Serde configuration, error handling, and topic access
     * @param table table to be wrapped
     * @return {@code ImprovedKTable}
     * @param <KR> type of keys in the table
     * @param <VR> type of values in the table
     */
    public <KR, VR> ImprovedKTable<KR, VR> wrap(final KTable<KR, VR> table) {
        if (table instanceof ImprovedKTable) {
            return (ImprovedKTable<KR, VR>) table;
        }
        return new ImprovedKTableImpl<>(table, this);
    }

    /**
     * Wrap a {@code KGroupedTable} and add methods to simplify Serde configuration, error handling, and topic access
     * @param table table to be wrapped
     * @return {@code ImprovedKGroupedTable}
     * @param <KR> type of keys in the table
     * @param <VR> type of values in the table
     */
    public <KR, VR> ImprovedKGroupedTable<KR, VR> wrap(final KGroupedTable<KR, VR> table) {
        if (table instanceof ImprovedKGroupedTable) {
            return (ImprovedKGroupedTable<KR, VR>) table;
        }
        return new ImprovedKGroupedTableImpl<>(table, this);
    }
}
