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
        if (stream instanceof KStreamXImpl) {
            // Kafka Streams internally casts KStream to KStreamImpl in some cases
            return ((KStreamXImpl<K, V>) stream).getWrapped();
        }
        return stream;
    }

    static <K, V> KGroupedStream<K, V> maybeUnwrap(final KGroupedStream<K, V> stream) {
        if (stream instanceof KGroupedStreamX) {
            // Kafka Streams internally casts KGroupedStream to KGroupedStreamImpl in some cases
            return ((KGroupedStreamXImpl<K, V>) stream).getWrapped();
        }
        return stream;
    }

    static <K, V> KTable<K, V> maybeUnwrap(final KTable<K, V> table) {
        if (table instanceof KTableXImpl) {
            // Kafka Streams internally casts KTable to KTableImpl in some cases
            return ((KTableXImpl<K, V>) table).getWrapped();
        }
        return table;
    }

    /**
     * Wrap a {@link KStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@link KStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> KStreamX<KR, VR> wrap(final KStream<KR, VR> stream) {
        if (stream instanceof KStreamX) {
            return (KStreamX<KR, VR>) stream;
        }
        return new KStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link KGroupedStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@link KGroupedStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> KGroupedStreamX<KR, VR> wrap(final KGroupedStream<KR, VR> stream) {
        if (stream instanceof KGroupedStreamX) {
            return (KGroupedStreamX<KR, VR>) stream;
        }
        return new KGroupedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link TimeWindowedKStream} and add methods to simplify Serde configuration, error handling, and topic
     * access
     * @param stream stream to be wrapped
     * @return {@link TimeWindowedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> TimeWindowedKStreamX<KR, VR> wrap(
            final TimeWindowedKStream<KR, VR> stream) {
        if (stream instanceof TimeWindowedKStreamX) {
            return (TimeWindowedKStreamX<KR, VR>) stream;
        }
        return new TimeWindowedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link SessionWindowedKStream} and add methods to simplify Serde configuration, error handling, and
     * topic access
     * @param stream stream to be wrapped
     * @return {@link SessionWindowedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> SessionWindowedKStreamX<KR, VR> wrap(
            final SessionWindowedKStream<KR, VR> stream) {
        if (stream instanceof SessionWindowedKStreamX) {
            return (SessionWindowedKStreamX<KR, VR>) stream;
        }
        return new SessionWindowedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link TimeWindowedCogroupedKStream} and add methods to simplify Serde configuration, error handling,
     * and topic access
     * @param stream stream to be wrapped
     * @return {@link TimeWindowedCogroupedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> TimeWindowedCogroupedKStreamX<KR, VR> wrap(
            final TimeWindowedCogroupedKStream<KR, VR> stream) {
        if (stream instanceof TimeWindowedCogroupedKStreamX) {
            return (TimeWindowedCogroupedKStreamX<KR, VR>) stream;
        }
        return new TimeWindowedCogroupedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link SessionWindowedCogroupedKStream} and add methods to simplify Serde configuration, error
     * handling, and topic access
     * @param stream stream to be wrapped
     * @return {@link SessionWindowedCogroupedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> SessionWindowedCogroupedKStreamX<KR, VR> wrap(
            final SessionWindowedCogroupedKStream<KR, VR> stream) {
        if (stream instanceof SessionWindowedCogroupedKStreamX) {
            return (SessionWindowedCogroupedKStreamX<KR, VR>) stream;
        }
        return new SessionWindowedCogroupedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link CogroupedKStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@link CogroupedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> CogroupedKStreamX<KR, VR> wrap(final CogroupedKStream<KR, VR> stream) {
        if (stream instanceof CogroupedKStreamX) {
            return (CogroupedKStreamX<KR, VR>) stream;
        }
        return new CogroupedStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link BranchedKStream} and add methods to simplify Serde configuration, error handling, and topic access
     * @param stream stream to be wrapped
     * @return {@link BranchedKStreamX}
     * @param <KR> type of keys in the stream
     * @param <VR> type of values in the stream
     */
    public <KR, VR> BranchedKStreamX<KR, VR> wrap(final BranchedKStream<KR, VR> stream) {
        if (stream instanceof BranchedKStreamX) {
            return (BranchedKStreamX<KR, VR>) stream;
        }
        return new BranchedKStreamXImpl<>(stream, this);
    }

    /**
     * Wrap a {@link KTable} and add methods to simplify Serde configuration, error handling, and topic access
     * @param table table to be wrapped
     * @return {@link KTableX}
     * @param <KR> type of keys in the table
     * @param <VR> type of values in the table
     */
    public <KR, VR> KTableX<KR, VR> wrap(final KTable<KR, VR> table) {
        if (table instanceof KTableX) {
            return (KTableX<KR, VR>) table;
        }
        return new KTableXImpl<>(table, this);
    }

    /**
     * Wrap a {@link KGroupedTable} and add methods to simplify Serde configuration, error handling, and topic access
     * @param table table to be wrapped
     * @return {@link KGroupedTableX}
     * @param <KR> type of keys in the table
     * @param <VR> type of values in the table
     */
    public <KR, VR> KGroupedTableX<KR, VR> wrap(final KGroupedTable<KR, VR> table) {
        if (table instanceof KGroupedTableX) {
            return (KGroupedTableX<KR, VR>) table;
        }
        return new KGroupedTableXImpl<>(table, this);
    }
}
