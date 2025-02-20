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

import static java.util.Collections.emptyMap;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindowedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class StreamsContextTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    private static StreamsContext newContext() {
        return new StreamsContext(StreamsTopicConfig.builder().build(), new Configurator(emptyMap()));
    }

    @Test
    void shouldNotReWrapStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KStreamX<Object, Object> wrapped = context.wrap(stream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapGroupedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KGroupedStream<Object, Object> groupedStream = stream.groupByKey();
        final KGroupedStreamX<Object, Object> wrapped = context.wrap(groupedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapTimeWindowedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KGroupedStream<Object, Object> groupedStream = stream.groupByKey();
        final TimeWindowedKStream<Object, Object> windowedStream =
                groupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1L)));
        final TimeWindowedKStreamX<Object, Object> wrapped = context.wrap(windowedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapSessionWindowedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KGroupedStream<Object, Object> groupedStream = stream.groupByKey();
        final SessionWindowedKStream<Object, Object> windowedStream =
                groupedStream.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(1L)));
        final SessionWindowedKStreamX<Object, Object> wrapped = context.wrap(windowedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapBranchedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final BranchedKStream<Object, Object> branchedStream = stream.split();
        final BranchedKStreamX<Object, Object> wrapped = context.wrap(branchedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapCogroupedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("topic");
        final KGroupedStream<String, String> groupedStream = stream.groupByKey();
        final CogroupedKStream<String, String> cogroupedStream = groupedStream.cogroup((k, v, a) -> a + v);
        final CogroupedKStreamX<String, String> wrapped = context.wrap(cogroupedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapTimeWindowedCogroupedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("topic");
        final KGroupedStream<String, String> groupedStream = stream.groupByKey();
        final CogroupedKStream<String, String> cogroupedStream = groupedStream.cogroup((k, v, a) -> a + v);
        final TimeWindowedCogroupedKStream<String, String> windowedCogroupedStream =
                cogroupedStream.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1L)));
        final TimeWindowedCogroupedKStreamX<String, String> wrapped = context.wrap(windowedCogroupedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapSessionWindowedCogroupedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream("topic");
        final KGroupedStream<String, String> groupedStream = stream.groupByKey();
        final CogroupedKStream<String, String> cogroupedStream = groupedStream.cogroup((k, v, a) -> a + v);
        final SessionWindowedCogroupedKStream<String, String> windowedCogroupedStream =
                cogroupedStream.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(1L)));
        final SessionWindowedCogroupedKStreamX<String, String> wrapped = context.wrap(windowedCogroupedStream);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapTable() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("topic");
        final KTableX<Object, Object> wrapped = context.wrap(table);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldNotReWrapGroupedTable() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("topic");
        final KGroupedTable<Object, Object> groupedTable = table.groupBy((k, v) -> KeyValue.pair(v, k));
        final KGroupedTableX<Object, Object> wrapped = context.wrap(groupedTable);
        this.softly.assertThat(context.wrap(wrapped)).isSameAs(wrapped);
    }

    @Test
    void shouldUnwrapStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KStreamX<Object, Object> wrapped = context.wrap(stream);
        this.softly.assertThat(StreamsContext.maybeUnwrap(wrapped)).isSameAs(stream);
    }

    @Test
    void shouldNotUnwrapStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        this.softly.assertThat(StreamsContext.maybeUnwrap(stream)).isSameAs(stream);
    }

    @Test
    void shouldUnwrapGroupedStream() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KGroupedStream<Object, Object> groupedStream = stream.groupByKey();
        final KGroupedStreamX<Object, Object> wrapped = context.wrap(groupedStream);
        this.softly.assertThat(StreamsContext.maybeUnwrap(wrapped)).isSameAs(groupedStream);
    }

    @Test
    void shouldNotUnwrapGroupedStream() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Object, Object> stream = builder.stream("topic");
        final KGroupedStream<Object, Object> groupedStream = stream.groupByKey();
        this.softly.assertThat(StreamsContext.maybeUnwrap(groupedStream)).isSameAs(groupedStream);
    }

    @Test
    void shouldUnwrapTable() {
        final StreamsContext context = newContext();
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("topic");
        final KTableX<Object, Object> wrapped = context.wrap(table);
        this.softly.assertThat(StreamsContext.maybeUnwrap(wrapped)).isSameAs(table);
    }

    @Test
    void shouldNotUnwrapTable() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<Object, Object> table = builder.table("topic");
        this.softly.assertThat(StreamsContext.maybeUnwrap(table)).isSameAs(table);
    }
}
