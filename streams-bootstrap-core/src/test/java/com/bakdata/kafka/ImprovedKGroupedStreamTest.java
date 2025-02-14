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

import static com.bakdata.kafka.ImprovedKStreamTest.startApp;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class ImprovedKGroupedStreamTest {

    @Test
    void shouldReduce() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final ImprovedKGroupedStream<String, String> grouped = input.groupByKey();
                final ImprovedKTable<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2);
                reduced.toStream().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("barbaz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldReduceUsingMaterialized() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKGroupedStream<Long, Long> grouped = input.groupByKey(
                        AutoGrouped.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKTable<Long, Long> reduced = grouped.reduce(Long::sum,
                        AutoMaterialized.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                reduced.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .add(1L, 2L)
                .add(1L, 3L);
        topology.streamOutput()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(2L)
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(5L)
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldAggregate() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final ImprovedKGroupedStream<String, String> grouped = input.groupByKey();
                final ImprovedKTable<String, String> aggregated =
                        grouped.aggregate(() -> "", (key, value, aggregate) -> aggregate + value);
                aggregated.toStream().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("barbaz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldAggregateUsingMaterialized() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKGroupedStream<Long, Long> grouped = input.groupByKey(
                        AutoGrouped.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKTable<Long, Long> aggregated =
                        grouped.aggregate(() -> 0L, (key, value, aggregate) -> aggregate + value,
                                AutoMaterialized.with(Preconfigured.create(Serdes.Long()),
                                        Preconfigured.create(Serdes.Long())));
                aggregated.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .add(1L, 2L)
                .add(1L, 3L);
        topology.streamOutput()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(2L)
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(5L)
                .expectNoMoreRecord();
        topology.stop();
    }
}
