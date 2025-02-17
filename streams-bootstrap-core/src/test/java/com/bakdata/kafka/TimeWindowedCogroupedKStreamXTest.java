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
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;

class TimeWindowedCogroupedKStreamXTest {

    @Test
    void shouldAggregate() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final CogroupedKStreamX<String, String> cogrouped =
                        grouped.cogroup((key, value, aggregate) -> aggregate + value);
                final TimeWindowedCogroupedKStreamX<String, String> windowed =
                        cogrouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60L)));
                final KTableX<Windowed<String>, String> aggregated = windowed.aggregate(() -> "");
                aggregated.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli()).to("output");
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
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:60000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final CogroupedKStreamX<String, String> cogrouped =
                        grouped.cogroup((key, value, aggregate) -> aggregate + value);
                final TimeWindowedCogroupedKStreamX<String, String> windowed =
                        cogrouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", Named.as("aggregate"));
                aggregated.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli()).to("output");
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
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:60000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final CogroupedKStreamX<String, String> cogrouped =
                        grouped.cogroup((key, value, aggregate) -> aggregate + value);
                final TimeWindowedCogroupedKStreamX<String, String> windowed =
                        cogrouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60L)));
                final KTableX<Windowed<String>, String> aggregated =
                        windowed.aggregate(() -> "", MaterializedX.with(Serdes.String(), Serdes.String()));
                aggregated.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli())
                        .to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:60000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldAggregateNamedUsingMaterialized() {
        final DoubleApp app = new DoubleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input =
                        builder.stream("input", ConsumedX.with(Serdes.String(), Serdes.String()));
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final CogroupedKStreamX<String, String> cogrouped =
                        grouped.cogroup((key, value, aggregate) -> aggregate + value);
                final TimeWindowedCogroupedKStreamX<String, String> windowed =
                        cogrouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60L)));
                final KTableX<Windowed<String>, String> aggregated = windowed.aggregate(() -> "", Named.as("aggregate"),
                        MaterializedX.with(Serdes.String(), Serdes.String()));
                aggregated.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli())
                        .to("output", ProducedX.with(Serdes.String(), Serdes.String()));
            }
        };
        try (final TestTopology<Double, Double> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .add("foo", "bar")
                    .at(Duration.ofSeconds(30L).toMillis())
                    .add("foo", "baz")
                    .at(Duration.ofSeconds(60L).toMillis())
                    .add("foo", "qux");
            topology.streamOutput()
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue("barbaz")
                    .expectNextRecord()
                    .hasKey("foo:60000")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }
}
