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

import static java.util.Collections.emptyMap;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.util.TopologyInformation;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class StreamJoinedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, String> stream = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> otherInput =
                        builder.stream("other_input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> joined =
                        stream.join(otherInput, (v1, v2) -> v1 + v2,
                                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                                StreamJoinedX.keySerde(Serdes.Long()));
                joined.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<Long, String> otherInput =
                        builder.stream("other_input", ConsumedX.keySerde(Serdes.Long()));
                final KStreamX<Long, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<Long, String, String>as("join").withKeySerde(Serdes.Long()));
                joined.to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined =
                        stream.join(otherInput, (v1, v2) -> v1 + v2,
                                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                                StreamJoinedX.valueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, Long, String>as("join").withValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<String, Long> otherInput =
                        builder.stream("other_input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, String> joined =
                        stream.join(otherInput, (v1, v2) -> v1 + v2,
                                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                                StreamJoinedX.otherValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<String, Long> otherInput =
                        builder.stream("other_input", ConsumedX.valueSerde(Serdes.Long()));
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, Long>as("join").withOtherValueSerde(Serdes.Long()));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
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
                final KStreamX<Long, Long> table =
                        builder.stream("other_input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined =
                        stream.join(table, Long::sum,
                                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                                StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
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
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseStoreName() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.as("join"));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getStores())
                    .anySatisfy(store -> this.softly.assertThat(store).startsWith("join"));
        }
    }

    @Test
    void shouldUseStoreNameModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withStoreName("join"));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getStores())
                    .anySatisfy(store -> this.softly.assertThat(store).startsWith("join"));
        }
    }

    @Test
    void shouldUseNameModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde()).withName("join"));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getProcessors())
                    .extracting(Node::name)
                    .anySatisfy(processor -> this.softly.assertThat(processor).startsWith("join"));
        }
    }

    @Test
    void shouldUseDslStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(StoreType.IN_MEMORY));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            final List<StateStore> stateStores = stores.stream()
                    .map(testDriver::getWindowStore)
                    .collect(Collectors.toList());
            this.softly.assertThat(stateStores)
                    .allSatisfy(stateStore -> this.softly.assertThat(stateStore.persistent()).isFalse());
        }
    }

    @Test
    void shouldUseDslStoreSupplierModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withDslStoreSuppliers(StoreType.IN_MEMORY));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            final List<StateStore> stateStores = stores.stream()
                    .map(testDriver::getWindowStore)
                    .collect(Collectors.toList());
            this.softly.assertThat(stateStores)
                    .allSatisfy(stateStore -> this.softly.assertThat(stateStore.persistent()).isFalse());
        }
    }

    @Test
    void shouldUseStoreSuppliers() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(
                                Stores.inMemoryWindowStore("this-store", Duration.ofMinutes(2L), Duration.ofMinutes(2L),
                                        true), Stores.inMemoryWindowStore("other-store", Duration.ofMinutes(2L),
                                        Duration.ofMinutes(2L), true)));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("this-store", "other-store");
        }
    }

    @Test
    void shouldUseThisStoreSuppliersModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withThisStoreSupplier(Stores.inMemoryWindowStore("store", Duration.ofMinutes(2L),
                                        Duration.ofMinutes(2L), true)));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldUseOtherStoreSuppliersModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withOtherStoreSupplier(Stores.inMemoryWindowStore("store", Duration.ofMinutes(2L),
                                        Duration.ofMinutes(2L), true)));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldDisableLogging() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withLoggingDisabled());
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            // TODO test topic existence. TestDriver does not expose it
        }
    }

    @Test
    void shouldEnableLogging() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> stream = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = stream.join(otherInput, (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.<String, String, String>keySerde(Preconfigured.defaultSerde())
                                .withLoggingEnabled(emptyMap()));
                joined.to("output");
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
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
            // TODO test topic config. TestDriver does not expose it
        }
    }

    //TODO retention
}
