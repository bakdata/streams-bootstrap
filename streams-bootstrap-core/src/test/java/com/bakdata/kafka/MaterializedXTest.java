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

import static com.bakdata.kafka.AsyncRunnable.runAsync;
import static com.bakdata.kafka.KafkaTest.POLL_TIMEOUT;
import static java.util.Collections.emptyMap;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopologyInformation;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.CachingKeyValueStore;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.KafkaContainer;

@ExtendWith(SoftAssertionsExtension.class)
class MaterializedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
            this.softly.assertThat(information.getStores())
                    .contains("store");
        }
    }

    @Test
    void shouldUseStoreType() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
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
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldUseWindowStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldUseSessionStoreSupplier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
            final TopologyInformation information = topology.getTopologyInformation();
            final List<String> stores = information.getStores();
            this.softly.assertThat(stores)
                    .contains("store");
        }
    }

    @Test
    void shouldDisableLogging(@TempDir final Path stateDir) {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>as("store")
                                .withLoggingDisabled());
                table.toStream().to("output");
            }
        };
        try (final KafkaContainer kafkaCluster = KafkaTest.newCluster()) {
            kafkaCluster.start();
            final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                    .bootstrapServers(kafkaCluster.getBootstrapServers())
                    .build();
            final KafkaTestClient testClient = new KafkaTestClient(endpointConfig);
            testClient.createTopic("input");
            testClient.createTopic("output");
            try (final ConfiguredStreamsApp<StreamsApp> configuredApp = app.configureApp(
                    TestTopologyFactory.createStreamsTestConfig(stateDir));
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
                runAsync(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from("output", POLL_TIMEOUT))
                        .hasSize(1)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                        });
                try (final ImprovedAdminClient admin = testClient.admin();
                        final TopicClient topicClient = admin.getTopicClient()) {
                    final String appId = new ImprovedStreamsConfig(executableApp.getConfig()).getAppId();
                    this.softly.assertThat(topicClient.exists(appId + "-store-changelog")).isFalse();
                }
            }
        }
    }

    @Test
    void shouldEnableLogging(@TempDir final Path stateDir) {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KTableX<String, String> table = builder.table("input",
                        MaterializedX.<String, String, KeyValueStore<Bytes, byte[]>>as("store")
                                .withLoggingEnabled(Map.of(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1")));
                table.toStream().to("output");
            }
        };
        try (final KafkaContainer kafkaCluster = KafkaTest.newCluster()) {
            kafkaCluster.start();
            final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                    .bootstrapServers(kafkaCluster.getBootstrapServers())
                    .build();
            final KafkaTestClient testClient = new KafkaTestClient(endpointConfig);
            testClient.createTopic("input");
            testClient.createTopic("output");
            try (final ConfiguredStreamsApp<StreamsApp> configuredApp = app.configureApp(
                    TestTopologyFactory.createStreamsTestConfig(stateDir));
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
                runAsync(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from("output", POLL_TIMEOUT))
                        .hasSize(1)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                        });
                try (final ImprovedAdminClient admin = testClient.admin();
                        final TopicClient topicClient = admin.getTopicClient()) {
                    final String appId = new ImprovedStreamsConfig(executableApp.getConfig()).getAppId();
                    final String topicName = appId + "-store-changelog";
                    this.softly.assertThat(topicClient.exists(topicName)).isTrue();
                    final Map<String, String> config = topicClient.getConfig(topicName);
                    this.softly.assertThat(config).containsEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1");
                }
            }
        }
    }

    @Test
    void shouldDisableCaching() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
                    .asInstanceOf(InstanceOfAssertFactories.type(WrappedStateStore.class))
                    .extracting(WrappedStateStore::wrapped)
                    .isNotInstanceOf(CachingKeyValueStore.class);
        }
    }

    @Test
    void shouldEnableCaching() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
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
                    .asInstanceOf(InstanceOfAssertFactories.type(WrappedStateStore.class))
                    .extracting(WrappedStateStore::wrapped)
                    .isInstanceOf(CachingKeyValueStore.class);
        }
    }

    @Test
    void shouldUseRetention() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final TimeWindowedKStreamX<String, String> windowed =
                        grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L)));
                final KTableX<Windowed<String>, Long> counted = windowed.count(
                        MaterializedX.<String, Long, WindowStore<Bytes, byte[]>>as("store")
                                .withRetention(Duration.ofMinutes(1L)));
                counted.toStream((k, v) -> k.key() + ":" + k.window().startTime().toEpochMilli()).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar")
                    .at(Duration.ofMinutes(2L).toMillis())
                    .add("foo", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo:0")
                    .hasValue(1L)
                    .expectNextRecord()
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldThrowIfRetentionIsTooShort() {
        final StreamsBuilderX builder = new StreamsBuilderX(StreamsTopicConfig.builder().build(), emptyMap());
        final KStreamX<String, String> input = builder.stream("input");
        final KGroupedStreamX<String, String> grouped = input.groupByKey();
        final TimeWindowedKStreamX<String, String> windowed =
                grouped.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L)));
        final MaterializedX<String, Long, WindowStore<Bytes, byte[]>> materialized =
                MaterializedX.<String, Long, WindowStore<Bytes, byte[]>>as("store")
                        .withRetention(Duration.ofSeconds(1L));
        this.softly.assertThatThrownBy(() -> windowed.count(materialized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The retention period of the window store store must be no smaller than its window size plus "
                        + "the grace period");
    }
}
