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

import static com.bakdata.kafka.KafkaTest.POLL_TIMEOUT;
import static com.bakdata.kafka.KafkaTest.SESSION_TIMEOUT;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.Preconfigured;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.ExecutableStreamsApp;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsRunner;
import com.bakdata.kafka.streams.test.StringApp;
import com.bakdata.kafka.util.TopologyInformation;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

@ExtendWith(SoftAssertionsExtension.class)
class ProducedXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldUseKeySerde() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<Long, String> input = builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                input.to("output", ProducedX.keySerde(Serdes.Long()));
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<Long, String> input =
                        builder.stream("input", ConsumedX.keySerde(Serdes.Long()));
                input.to("output", ProducedX.<Long, String>as("output").withKeySerde(Serdes.Long()));
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, Long> input = builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                input.to("output", ProducedX.valueSerde(Serdes.Long()));
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, Long> input =
                        builder.stream("input", ConsumedX.valueSerde(Serdes.Long()));
                input.to("output", ProducedX.<String, Long>as("output").withValueSerde(Serdes.Long()));
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                input.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
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
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.to("output", ProducedX.as("output"));
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
            final TopologyInformation information = topology.getTopologyInformation();
            this.softly.assertThat(information.getSinks())
                    .extracting(Node::name)
                    .contains("output");
        }
    }

    @Test
    void shouldUseNameModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.to("output", ProducedX.<String, String>keySerde(Preconfigured.defaultSerde()).withName("output"));
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
            final TopologyInformation information = topology.getTopologyInformation();
            this.softly.assertThat(information.getSinks())
                    .extracting(Node::name)
                    .contains("output");
        }
    }

    @Test
    void shouldUseStreamPartitioner() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.to("output",
                        ProducedX.streamPartitioner((topic, key, value, numPartitions) -> {
                            final int partition = "bar".equals(value) ? 0 : 1;
                            return Optional.of(Set.of(partition));
                        }));
            }
        };
        try (final KafkaContainer kafkaCluster = KafkaTest.newCluster()) {
            kafkaCluster.start();
            final RuntimeConfiguration configuration = RuntimeConfiguration.create(kafkaCluster.getBootstrapServers())
                            .withNoStateStoreCaching()
                            .withSessionTimeout(SESSION_TIMEOUT);
            final KafkaTestClient testClient = new KafkaTestClient(configuration);
            testClient.createTopic("input");
            testClient.createTopic("output", KafkaTestClient.defaultTopicSettings().partitions(2).build());
            try (final ConfiguredStreamsApp<StreamsApp> configuredApp = app.configureApp();
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp
                            .withRuntimeConfiguration(configuration);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .withKeySerializer(new StringSerializer())
                        .withValueSerializer(new StringSerializer())
                        .to("input", List.of(
                                new SimpleProducerRecord<>("foo", "bar"),
                                new SimpleProducerRecord<>("foo", "baz")
                        ));
                runAsync(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .withKeyDeserializer(new StringDeserializer())
                                .withValueDeserializer(new StringDeserializer())
                                .from("output", POLL_TIMEOUT))
                        .hasSize(2)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                            this.softly.assertThat(outputRecord.partition()).isEqualTo(0);
                        })
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("baz");
                            this.softly.assertThat(outputRecord.partition()).isEqualTo(1);
                        });
            }
        }
    }

    @Test
    void shouldUseStreamPartitionerModifier() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.to("output", ProducedX.<String, String>as("output")
                        .withStreamPartitioner((topic, key, value, numPartitions) -> {
                            final int partition = "bar".equals(value) ? 0 : 1;
                            return Optional.of(Set.of(partition));
                        }));
            }
        };
        try (final KafkaContainer kafkaCluster = KafkaTest.newCluster()) {
            kafkaCluster.start();
            final RuntimeConfiguration configuration = RuntimeConfiguration.create(kafkaCluster.getBootstrapServers())
                            .withNoStateStoreCaching()
                            .withSessionTimeout(SESSION_TIMEOUT);
            final KafkaTestClient testClient = new KafkaTestClient(configuration);
            testClient.createTopic("input");
            testClient.createTopic("output", KafkaTestClient.defaultTopicSettings().partitions(2).build());
            try (final ConfiguredStreamsApp<StreamsApp> configuredApp = app.configureApp();
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withRuntimeConfiguration(
                            configuration);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .withKeySerializer(new StringSerializer())
                        .withValueSerializer(new StringSerializer())
                        .to("input", List.of(
                                new SimpleProducerRecord<>("foo", "bar"),
                                new SimpleProducerRecord<>("foo", "baz")
                        ));
                runAsync(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .withKeyDeserializer(new StringDeserializer())
                                .withValueDeserializer(new StringDeserializer())
                                .from("output", POLL_TIMEOUT))
                        .hasSize(2)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                            this.softly.assertThat(outputRecord.partition()).isEqualTo(0);
                        })
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("baz");
                            this.softly.assertThat(outputRecord.partition()).isEqualTo(1);
                        });
            }
        }
    }
}
