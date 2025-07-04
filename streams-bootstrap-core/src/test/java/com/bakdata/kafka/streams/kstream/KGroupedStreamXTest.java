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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;

class KGroupedStreamXTest {

    @Test
    void shouldCount() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, Long> counted = grouped.count();
                counted.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
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
                final KTableX<String, Long> counted = grouped.count(Named.as("count"));
                counted.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
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
                final KTableX<String, Long> counted =
                        grouped.count(MaterializedX.keySerde(Serdes.String()));
                counted.toStream().to("output", ProducedX.keySerde(Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
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
                final KTableX<String, Long> counted = grouped.count(Named.as("count"),
                        MaterializedX.with(Serdes.String(), Serdes.Long()));
                counted.toStream().to("output", ProducedX.keySerde(Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
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
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2);
                reduced.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2,
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
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
    void shouldReduceNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, String> reduced = grouped.reduce((value1, value2) -> value1 + value2,
                        Named.as("reduce"), MaterializedX.with(Serdes.String(), Serdes.String()));
                reduced.toStream().to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
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
    void shouldAggregate() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, String> aggregated =
                        grouped.aggregate(() -> "", (key, value, aggregate) -> aggregate + value);
                aggregated.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
                final KTableX<String, String> aggregated =
                        grouped.aggregate(() -> "", (key, value, aggregate) -> aggregate + value,
                                MaterializedX.with(Serdes.String(), Serdes.String()));
                aggregated.toStream()
                        .to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
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
    void shouldAggregateNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, String> aggregated =
                        grouped.aggregate(() -> "", (key, value, aggregate) -> aggregate + value,
                                Named.as("aggregate"), MaterializedX.with(Serdes.String(), Serdes.String()));
                aggregated.toStream()
                        .to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
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
    void shouldTimeWindow() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final TimeWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count();
                final KStreamX<String, Long> output =
                        counted.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:60000")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldSlidingWindow() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final TimeWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(30L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count();
                final KStreamX<String, Long> output =
                        counted.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli());
                output.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey("foo:1")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30001")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo:30000")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldSessionWindow() {
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
    void shouldCogroup() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final CogroupedKStreamX<String, String> cogrouped =
                        grouped.cogroup((key, value, aggregate) -> aggregate + value);
                final KTableX<String, String> aggregated = cogrouped.aggregate(() -> "");
                aggregated.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
        }
    }
}
