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
        final Predicate<Long, Long> predicate = mock();
        when(predicate.test(1L, 2L)).thenReturn(true);
        when(predicate.test(3L, 4L)).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> filtered = input.filter(predicate,
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                filtered.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 4L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(3L)
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNamedUsingMaterialized() {
        final Predicate<Long, Long> predicate = mock();
        when(predicate.test(1L, 2L)).thenReturn(true);
        when(predicate.test(3L, 4L)).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> filtered = input.filter(predicate, Named.as("filter"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                filtered.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 4L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(3L)
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
        final Predicate<Long, Long> predicate = mock();
        when(predicate.test(1L, 2L)).thenReturn(false);
        when(predicate.test(3L, 4L)).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> filtered = input.filterNot(predicate,
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                filtered.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 4L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(3L)
                    .hasValue(null)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNotNamedUsingMaterialized() {
        final Predicate<Long, Long> predicate = mock();
        when(predicate.test(1L, 2L)).thenReturn(false);
        when(predicate.test(3L, 4L)).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> filtered = input.filterNot(predicate, Named.as("filter"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                filtered.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 4L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(3L)
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
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KGroupedTableX<Long, Long> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k),
                        GroupedX.with(Serdes.Long(), Serdes.Long()));
                grouped.count().toStream()
                        .to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(2L)
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey(2L)
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
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.join(otherInput, Long::sum,
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinNamedUsingMaterialized() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.join(otherInput, Long::sum, Named.as("join"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
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
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.leftJoin(otherInput, (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
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
        }
    }

    @Test
    void shouldLeftJoinNamedUsingMaterialized() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        Named.as("join"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
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
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.outerJoin(otherInput, (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(3L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinNamedUsingMaterialized() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> input =
                        builder.table("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> otherInput = builder.table("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        Named.as("join"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                joined.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(3L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }
}
