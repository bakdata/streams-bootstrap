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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class MaterializedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, String> table = builder.table("input", MaterializedX.keySerde(Serdes.Long()));
                table.toStream().to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseKeySerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, String> table = builder.table("input",
                        MaterializedX.<Long, String, KeyValueStore<Bytes, byte[]>>as("store")
                                .withKeySerde(Serdes.Long()));
                table.toStream().to("output", ProducedX.keySerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .add(1L, "bar");
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, Long> table = builder.table("input", MaterializedX.valueSerde(Serdes.Long()));
                table.toStream().to("output", ProducedX.valueSerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 2L);
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseValueSerdeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, Long> table = builder.table("input",
                        MaterializedX.<String, Long, KeyValueStore<Bytes, byte[]>>as("store")
                                .withValueSerde(Serdes.Long()));
                table.toStream().to("output", ProducedX.valueSerde(Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withValueSerde(Serdes.Long())
                    .add("foo", 2L);
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseSerdes() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<Long, Long> table =
                        builder.table("input", MaterializedX.with(Serdes.Long(), Serdes.Long()));
                table.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
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
                final KTableX<String, String> table = builder.table("input", MaterializedX.as("store"));
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getStores())
                    .contains("store");
        }
    }

    @Test
    void shouldUseStoreType() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input", MaterializedX.as(StoreType.IN_MEMORY));
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            final List<StateStore> stateStores = stores.stream()
                    .map(testDriver::getKeyValueStore)
                    .collect(Collectors.toList());
            this.softly.assertThat(stateStores)
                    .allSatisfy(stateStore -> this.softly.assertThat(stateStore.persistent()).isFalse());
        }
    }

    @Test
    void shouldUseStoreTypeModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>keySerde(
                                Preconfigured.defaultSerde()).withStoreType(StoreType.IN_MEMORY));
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            final List<StateStore> stateStores = stores.stream()
                    .map(testDriver::getKeyValueStore)
                    .collect(Collectors.toList());
            this.softly.assertThat(stateStores)
                    .allSatisfy(stateStore -> this.softly.assertThat(stateStore.persistent()).isFalse());
        }
    }

    @Test
    void shouldUseKeyValueStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.as(Stores.inMemoryKeyValueStore("store")));
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldUseWindowStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final TimeWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count(MaterializedX.as(
                        Stores.inMemoryWindowStore("store", Duration.ofMinutes(1L), Duration.ofMinutes(1L), false)));
                counted.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli()).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(1L)
                    .expectNoMoreRecord();
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldUseSessionStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final SessionWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count(MaterializedX.as(
                        Stores.inMemorySessionStore("store", Duration.ofMinutes(1L))));
                counted.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli()).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(1L)
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
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>keySerde(
                                Preconfigured.defaultSerde()).withLoggingDisabled());
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test topic existence. TestDriver does not expose it
        }
    }

    @Test
    void shouldEnableLogging() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>keySerde(
                                Preconfigured.defaultSerde()).withLoggingEnabled(emptyMap()));
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            // TODO test topic config. TestDriver does not expose it
        }
    }

    @Test
    void shouldDisableCaching() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingDisabled());
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            this.softly.assertThat(testDriver.getTimestampedKeyValueStore("store"))
                    .asInstanceOf(InstanceOfAssertFactories.type(MeteredTimestampedKeyValueStore.class))
                    .extracting(WrappedStateStore::wrapped)
                    .isNotInstanceOf(CachingKeyValueStore.class);
        }
    }

    @Test
    void shouldEnableCaching() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>as("store").withCachingEnabled());
                table.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            final TopologyTestDriver testDriver = topology.getTestDriver();
            this.softly.assertThat(testDriver.getTimestampedKeyValueStore("store"))
                    .asInstanceOf(InstanceOfAssertFactories.type(MeteredTimestampedKeyValueStore.class))
                    .extracting(WrappedStateStore::wrapped)
                    .isInstanceOf(CachingKeyValueStore.class);
        }
    }
}
