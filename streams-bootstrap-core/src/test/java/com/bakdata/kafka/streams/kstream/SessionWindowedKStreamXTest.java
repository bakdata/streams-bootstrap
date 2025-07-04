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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.streams.apps.DoubleApp;
import com.bakdata.kafka.streams.apps.StringApp;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;

class SessionWindowedKStreamXTest {

    @Test
    void shouldCount() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count();
                final KStreamX<String, Long> output = counted.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldCountNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count(Named.as("count"));
                final KStreamX<String, Long> output = counted.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldCountUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted =
                        windowed.count(MaterializedX.with(Serdes.String(), Serdes.Long()));
                final KStreamX<String, Long> output = counted.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.keySerde(Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldCountNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted =
                        windowed.count(Named.as("count"), MaterializedX.with(Serdes.String(), Serdes.Long()));
                final KStreamX<String, Long> output = counted.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.keySerde(Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregate() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", (k, v, a) -> a + v, (k, a1, a2) -> a1 + a2);
                final KStreamX<String, String> output = aggregated.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", (k, v, a) -> a + v, (k, a1, a2) -> a1 + a2, Named.as("aggregate"));
                final KStreamX<String, String> output = aggregated.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", (k, v, a) -> a + v, (k, a1, a2) -> a1 + a2,
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                final KStreamX<String, String> output = aggregated.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", (k, v, a) -> a + v, (k, a1, a2) -> a1 + a2, Named.as("aggregate"),
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                final KStreamX<String, String> output = aggregated.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduce() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> reduced = windowed.reduce((v1, v2) -> v1 + v2);
                final KStreamX<String, String> output = reduced.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduceNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> reduced =
                        windowed.reduce((v1, v2) -> v1 + v2, Named.as("reduce"));
                final KStreamX<String, String> output = reduced.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduceUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> reduced =
                        windowed.reduce((v1, v2) -> v1 + v2, MaterializedX.with(Serdes.String(), Serdes.String()));
                final KStreamX<String, String> output = reduced.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldReduceNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, String> reduced =
                        windowed.reduce((v1, v2) -> v1 + v2, Named.as("reduce"),
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                final KStreamX<String, String> output = reduced.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:30000:30000")
                    .hasValue(null)
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:91000:91000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseEmitStrategy() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted =
                        windowed.emitStrategy(EmitStrategy.onWindowClose()).count();
                final KStreamX<String, Long> output = counted.toStream(
                        (k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli() + ":" + k.window().endTime()
                                .toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    // if time is lower than session size, there is a bug in Kafka Streams
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(91L).toMillis()) // trigger flush
                    .add("foo", "qux");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:30000:60000")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }
}
