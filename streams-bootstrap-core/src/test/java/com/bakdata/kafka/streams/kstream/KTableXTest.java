/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.Preconfigured;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.apps.DoubleApp;
import com.bakdata.kafka.streams.apps.StringApp;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class KTableXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldConvertToStream() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedTableX<String, String> grouped = input.groupBy((k, v) -> KeyValue.pair(v, k),
                        GroupedX.with(Serdes.String(), Serdes.String()));
                grouped.count().toStream()
                        .to("output", ProducedX.keySerde(Serdes.String()));
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
    void shouldTransformValues() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        if ("foo".equals(readOnlyKey) && "bar".equals(value)) {
                            return "baz";
                        }
                        throw new UnsupportedOperationException();
                    }
                };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> transformed = input.transformValues(transformer);
                transformed.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldTransformValuesNamed() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        if ("foo".equals(readOnlyKey) && "bar".equals(value)) {
                            return "baz";
                        }
                        throw new UnsupportedOperationException();
                    }
                };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> transformed = input.transformValues(transformer, Named.as("transform"));
                transformed.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldTransformValuesUsingMaterialized() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        if ("foo".equals(readOnlyKey) && "bar".equals(value)) {
                            return "baz";
                        }
                        throw new UnsupportedOperationException();
                    }
                };
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> transformed =
                        input.transformValues(transformer, MaterializedX.with(Serdes.String(), Serdes.String()));
                transformed.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
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
    void shouldTransformValuesNamedUsingMaterialized() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        if ("foo".equals(readOnlyKey) && "bar".equals(value)) {
                            return "baz";
                        }
                        throw new UnsupportedOperationException();
                    }
                };
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> transformed =
                        input.transformValues(transformer, MaterializedX.with(Serdes.String(), Serdes.String()),
                                Named.as("transform"));
                transformed.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
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
    void shouldTransformValuesUsingStore() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        final KeyValueStore<String, String> store = this.getStateStore("my-store");
                        store.put(readOnlyKey, value);
                        return value;
                    }
                };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> transformed = input.transformValues(transformer, "my-store");
                transformed.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldTransformValuesNamedUsingStore() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        final KeyValueStore<String, String> store = this.getStateStore("my-store");
                        store.put(readOnlyKey, value);
                        return value;
                    }
                };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> transformed =
                        input.transformValues(transformer, Named.as("transform"), "my-store");
                transformed.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldTransformValuesUsingStoreAndMaterialized() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        final KeyValueStore<String, String> store = this.getStateStore("my-store");
                        store.put(readOnlyKey, value);
                        return value;
                    }
                };
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Serdes.String(),
                                Serdes.String());
                builder.addStateStore(store);
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> transformed =
                        input.transformValues(transformer, MaterializedX.with(Serdes.String(), Serdes.String()),
                                "my-store");
                transformed.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldTransformValuesNamedUsingStoreAndMaterialized() {
        final ValueTransformerWithKeySupplier<String, String, String> transformer =
                () -> new SimpleValueTransformerWithKey<>() {

                    @Override
                    public String transform(final String readOnlyKey, final String value) {
                        final KeyValueStore<String, String> store = this.getStateStore("my-store");
                        store.put(readOnlyKey, value);
                        return value;
                    }
                };
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Serdes.String(),
                                Serdes.String());
                builder.addStateStore(store);
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> transformed =
                        input.transformValues(transformer, MaterializedX.with(Serdes.String(), Serdes.String()),
                                Named.as("transform"), "my-store");
                transformed.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldFKeyJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.join(otherInput, Function.identity(), (v1, v2) -> v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFKeyJoinTableJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined =
                        input.join(otherInput, Function.identity(), (v1, v2) -> v1 + v2, TableJoined.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFKeyJoinUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.join(otherInput, Function.identity(), (v1, v2) -> v1 + v2,
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
                    .add("bar", "baz");
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
    void shouldFKeyJoinTableJoinedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.join(otherInput, Function.identity(), (v1, v2) -> v1 + v2, TableJoined.as("join"),
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
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput, Function.identity(),
                        (v1, v2) -> v2 == null ? v1 : v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinTableJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput, Function.identity(),
                        (v1, v2) -> v2 == null ? v1 : v1 + v2, TableJoined.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.leftJoin(otherInput, Function.identity(), (v1, v2) -> v2 == null ? v1 : v1 + v2,
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
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinTableJoinedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.leftJoin(otherInput, Function.identity(), (v1, v2) -> v2 == null ? v1 : v1 + v2,
                                TableJoined.as("join"), MaterializedX.with(Serdes.String(), Serdes.String()));
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
                    .add("bar", "baz");
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
    void shouldFKeyJoinWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.join(otherInput, (k, v) -> v, (v1, v2) -> v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFKeyJoinTableJoinedWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined =
                        input.join(otherInput, (k, v) -> v, (v1, v2) -> v1 + v2, TableJoined.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFKeyJoinWithKeyUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined = input.join(otherInput, (k, v) -> v, (v1, v2) -> v1 + v2,
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
                    .add("bar", "baz");
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
    void shouldFKeyJoinTableJoinedWithKeyUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.join(otherInput, (k, v) -> v, (v1, v2) -> v1 + v2, TableJoined.as("join"),
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
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput, (k, v) -> v,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2);
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinTableJoinedWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> otherInput = builder.table("other_input");
                final KTableX<String, String> joined = input.leftJoin(otherInput, (k, v) -> v,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2, TableJoined.as("join"));
                joined.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinWithKeyUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.leftJoin(otherInput, (k, v) -> v, (v1, v2) -> v2 == null ? v1 : v1 + v2,
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
                    .add("bar", "baz");
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
    void shouldLeftFKeyJoinTableJoinedWithKeyUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input =
                        builder.table("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> otherInput =
                        builder.table("other_input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KTableX<String, String> joined =
                        input.leftJoin(otherInput, (k, v) -> v, (v1, v2) -> v2 == null ? v1 : v1 + v2,
                                TableJoined.as("join"), MaterializedX.with(Serdes.String(), Serdes.String()));
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
                    .add("bar", "baz");
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
    void shouldSuppress() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> input = builder.table("input");
                final KTableX<String, String> suppressed =
                        input.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(60L), BufferConfig.unbounded()));
                suppressed.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz")
                    .at(60000L)
                    .add("baz", "qux"); // trigger flush
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldHaveQueryableStoreName() {
        final StreamsBuilderX builder = new StreamsBuilderX(StreamsTopicConfig.builder().build(), Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "app-id",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        ));
        final KTableX<Object, Object> table = builder.stream("input").toTable(Materialized.as("store"));
        this.softly.assertThat(table.queryableStoreName()).isEqualTo("store");
    }

}
