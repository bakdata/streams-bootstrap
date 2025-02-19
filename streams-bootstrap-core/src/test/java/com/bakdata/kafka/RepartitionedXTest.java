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

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.util.TopologyInformation;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Sink;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class RepartitionedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, String> input = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> repartitioned = input.repartition(RepartitionedX.keySerde(Serdes.Long()));
                repartitioned.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .add(1L, "foo");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("foo")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseKeySerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, String> input = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> repartitioned =
                        input.repartition(RepartitionedX.<Long, String>as("repartition").withKeySerde(Serdes.Long()));
                repartitioned.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .add(1L, "foo");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("foo")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, Long> input = builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, Long> repartitioned =
                        input.repartition(RepartitionedX.valueSerde(Serdes.Long()));
                repartitioned.to("output", ProducedX.valueSerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, Long> input =
                        builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, Long> repartitioned =
                        input.repartition(RepartitionedX.<String, Long>as("repartition").withValueSerde(Serdes.Long()));
                repartitioned.to("output", ProducedX.valueSerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withValueSerde(Serdes.Long())
                    .add("foo", 1L);
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseSerdes() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> repartitioned =
                        input.repartition(RepartitionedX.with(Serdes.Long(), Serdes.Long()));
                repartitioned.repartition(RepartitionedX.keySerde(Serdes.Long()))
                        .to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseName() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(RepartitionedX.as("repartition"));
                repartitioned.to("output");
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
            final List<Node> nodes = TopologyInformation.getNodes(topology.getTopologyDescription());
            this.softly.assertThat(nodes)
                    .anySatisfy(node -> this.softly.assertThat(node)
                            .asInstanceOf(type(Sink.class))
                            .satisfies(
                                    sink -> this.softly.assertThat(sink.topic()).isEqualTo("repartition-repartition")));
        }
    }

    @Test
    void shouldUseNameModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(
                        RepartitionedX.<String, String>keySerde(Preconfigured.defaultSerde()).withName("repartition"));
                repartitioned.to("output");
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
            final List<Node> nodes = TopologyInformation.getNodes(topology.getTopologyDescription());
            this.softly.assertThat(nodes)
                    .anySatisfy(node -> this.softly.assertThat(node)
                            .asInstanceOf(type(Sink.class))
                            .satisfies(
                                    sink -> this.softly.assertThat(sink.topic()).isEqualTo("repartition-repartition")));
        }
    }

    @Test
    void shouldUseStreamPartitioner() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned =
                        input.repartition(RepartitionedX.streamPartitioner((topic, key, value, numPartitions) -> 1));
                repartitioned.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test partition. TestDriver does not expose it
        }
    }

    @Test
    void shouldUseStreamPartitionerModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(
                        RepartitionedX.<String, String>as("repartition")
                                .withStreamPartitioner((topic, key, value, numPartitions) -> 1));
                repartitioned.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test partition. TestDriver does not expose it
        }
    }

    @Test
    void shouldUseNumberOfPartitions() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(RepartitionedX.numberOfPartitions(2));
                repartitioned.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test partition. TestDriver does not expose it
        }
    }

    @Test
    void shouldUseNumberOfPartitionsModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned =
                        input.repartition(RepartitionedX.<String, String>as("repartition").withNumberOfPartitions(2));
                repartitioned.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test partition. TestDriver does not expose it
        }
    }
}
