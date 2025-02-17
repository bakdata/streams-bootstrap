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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.junit.jupiter.api.Test;

class KTableXTest {

    @Test
    void shouldConvertToStream() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                input.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldConvertToStreamNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                input.toStream(Named.as("toStream")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldConvertToStreamMapping() {
        final KeyValueMapper<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                input.toStream(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldConvertToStreamMappingNamed() {
        final KeyValueMapper<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                input.toStream(mapper, Named.as("toStream")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilter() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("baz", "qux")).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> filtered = input.filter(predicate);
                filtered.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("baz", "qux")).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> filtered = input.filter(predicate, Named.as("filter"));
                filtered.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterUsingMaterialized() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("baz", "qux")).thenReturn(false);
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> filtered = input.filter(predicate,
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                filtered.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNamedUsingMaterialized() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("baz", "qux")).thenReturn(false);
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> filtered = input.filter(predicate, Named.as("filter"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                filtered.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNot() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("baz", "qux")).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> filtered = input.filterNot(predicate);
                filtered.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNotNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("baz", "qux")).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> filtered = input.filterNot(predicate, Named.as("filter"));
                filtered.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNotUsingMaterialized() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("baz", "qux")).thenReturn(true);
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> filtered = input.filterNot(predicate,
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                filtered.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNotNamedUsingMaterialized() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("baz", "qux")).thenReturn(true);
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> filtered = input.filterNot(predicate, Named.as("filter"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                filtered.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGroupBy() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KGroupedTableX<String, String> grouped =
                        input.groupBy((k, v) -> KeyValue.pair(v, k));
                grouped.count().toStream().to("output");
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
    void shouldGroupByUsingGrouped() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k),
                        GroupedX.with(Serdes.String(), Serdes.String()));
                grouped.count().toStream()
                        .to("output", ProducedX.with(Serdes.String(), Serdes.Long()));
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
    void shouldJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.join(otherInput, (v1, v2) -> v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.join(otherInput, (v1, v2) -> v1 + v2, Named.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.join(otherInput, (v1, v2) -> v1 + v2,
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.join(otherInput, (v1, v2) -> v1 + v2, Named.as("join"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2, Named.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.leftJoin(otherInput, (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        Named.as("join"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .add("foo", "baz");
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2, Named.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .add("foo", "baz");
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.outerJoin(otherInput, (v1, v2) -> v1 == null ? v2 : v1 + v2,
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        Named.as("join"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                joined.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "baz");
            topology.input("input")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValues() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> mapped = input.mapValues(mapper);
                mapped.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesNamed() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> mapped = input.mapValues(mapper, Named.as("map"));
                mapped.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesUsingMaterialized() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> mapped =
                        input.mapValues(mapper, MaterializedX.with(Serdes.String(), Serdes.String()));
                mapped.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesNamedUsingMaterialized() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> mapped = input.mapValues(mapper, Named.as("mapped"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                mapped.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKey() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> mapped = input.mapValues(mapper);
                mapped.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKeyNamed() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> mapped = input.mapValues(mapper, Named.as("map"));
                mapped.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKeyUsingMaterialized() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> mapped =
                        input.mapValues(mapper, MaterializedX.with(Serdes.String(), Serdes.String()));
                mapped.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKeyNamedUsingMaterialized() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> mapped = input.mapValues(mapper, Named.as("mapped"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                mapped.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }
}
