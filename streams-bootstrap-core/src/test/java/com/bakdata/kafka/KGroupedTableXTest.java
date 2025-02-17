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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Named;
import org.junit.jupiter.api.Test;

class KGroupedTableXTest {

    @Test
    void shouldCount() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k));
                final KTableX<String, Long> counted = grouped.count();
                counted.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldCountNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k));
                final KTableX<String, Long> counted = grouped.count(Named.as("count"));
                counted.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldCountUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k), GroupedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, Long> counted = grouped.count(MaterializedX.keySerde(Serdes.String()));
                counted.toStream().to("output", ProducedX.keySerde(Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduce() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k));
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2,
                        (value1, value2) -> value1.replace(value2, ""));
                reduced.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduceUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k), GroupedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2,
                        (value1, value2) -> value1.replace(value2, ""),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduceNamedMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k), GroupedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2,
                        (value1, value2) -> value1.replace(value2, ""), Named.as("reduce"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregate() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k));
                final KTableX<String, String> reduced =
                        grouped.aggregate(() -> "", (k, v, a) -> a + v, (k, v, a) -> a.replace(v, ""));
                reduced.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k));
                final KTableX<String, String> reduced =
                        grouped.aggregate(() -> "", (k, v, a) -> a + v, (k, v, a) -> a.replace(v, ""),
                                Named.as("aggregate"));
                reduced.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k), GroupedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> reduced =
                        grouped.aggregate(() -> "", (k, v, a) -> a + v, (k, v, a) -> a.replace(v, ""),
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k), GroupedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> reduced =
                        grouped.aggregate(() -> "", (k, v, a) -> a + v, (k, v, a) -> a.replace(v, ""),
                                Named.as("aggregate"), MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foobaz")
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("foo")
                    .expectNextRecord()
                    .hasKey("qux")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }
}
