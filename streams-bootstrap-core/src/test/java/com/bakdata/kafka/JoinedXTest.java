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
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class JoinedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, String> stream = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                final KTableX<Long, String> table = builder.table("table_input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> joined =
                        stream.join(table, (v1, v2) -> v1 + v2, JoinedX.keySerde(Serdes.Long()));
                joined.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "baz");
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseKeySerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, String> stream = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                final KTableX<Long, String> table = builder.table("table_input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> joined = stream.join(table, (v1, v2) -> v1 + v2,
                        JoinedX.<Long, String, String>as("join").withKeySerde(Serdes.Long()));
                joined.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "baz");
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, Long> stream = builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                final KTableX<String, String> table = builder.table("table_input");
                final KStreamX<String, String> joined =
                        stream.join(table, (v1, v2) -> v1 + v2, JoinedX.valueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .add("foo", "baz");
            topology.input("input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("1baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, Long> stream = builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                final KTableX<String, String> table = builder.table("table_input");
                final KStreamX<String, String> joined = stream.join(table, (v1, v2) -> v1 + v2,
                        JoinedX.<String, Long, String>as("join").withValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .add("foo", "baz");
            topology.input("input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("1baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseOtherValueSerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KTableX<String, Long> table = builder.table("table_input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, String> joined =
                        stream.join(table, (v1, v2) -> v1 + v2, JoinedX.otherValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar1")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseOtherValueSerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KTableX<String, Long> table = builder.table("table_input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, String> joined = stream.join(table, (v1, v2) -> v1 + v2,
                        JoinedX.<String, String, Long>as("join").withOtherValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar1")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseSerdes() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> stream =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> table =
                        builder.table("table_input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined =
                        stream.join(table, Long::sum, JoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
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
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseName() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KTableX<String, String> table = builder.table("table_input");
                final KStreamX<String, String> joined = stream.join(table, (v1, v2) -> v1 + v2, JoinedX.as("join"));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .add("foo", "baz");
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseNameModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KTableX<String, String> table = builder.table("table_input");
                final KStreamX<String, String> joined = stream.join(table, (v1, v2) -> v1 + v2,
                        JoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde()).withName("join"));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .add("foo", "baz");
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseGracePeriodModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KTableX<String, String> table = builder.table("table_input",
                        MaterializedX.as(
                                Stores.persistentVersionedKeyValueStore("store", Duration.ofSeconds(1L))));
                final KStreamX<String, String> joined = stream.join(table, (v1, v2) -> v1 + v2,
                        JoinedX.<String, String, String>as("join").withGracePeriod(Duration.ofSeconds(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("table_input")
                    .at(0L)
                    .add("foo", "baz");
            topology.input("input")
                    .at(1000L)  //advance stream time
                    .add("", "");
            topology.input("table_input")
                    .at(1000L)
                    .add("foo", "qux");
            topology.input("input")
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }
}
