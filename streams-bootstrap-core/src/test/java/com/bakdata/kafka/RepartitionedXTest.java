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

import static com.bakdata.kafka.KafkaTest.POLL_TIMEOUT;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicSettings;
import com.bakdata.kafka.util.TopologyInformation;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

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
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getInternalTopics())
                    .anySatisfy(topic -> this.softly.assertThat(topic).endsWith("repartition-repartition"));
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
            final TopologyInformation information = TestTopologyFactory.getTopologyInformation(topology);
            this.softly.assertThat(information.getInternalTopics())
                    .anySatisfy(topic -> this.softly.assertThat(topic).endsWith("repartition-repartition"));
        }
    }

    @Test
    void shouldUseStreamPartitioner() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(RepartitionedX
                        .<String, String>streamPartitioner(
                                (topic, key, value, numPartitions) -> "bar".equals(value) ? 0 : 1)
                        .withNumberOfPartitions(2)
                        .withName("repartition"));
                repartitioned.to("output");
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
                    TestTopologyFactory.createStreamsTestConfig());
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(
                                new SimpleProducerRecord<>("foo", "bar"),
                                new SimpleProducerRecord<>("foo", "baz")
                        ));
                TestHelper.run(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from(new ImprovedStreamsConfig(executableApp.getConfig()).getAppId()
                                      + "-repartition-repartition", POLL_TIMEOUT))
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
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from("output", POLL_TIMEOUT))
                        .hasSize(2)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                        })
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("baz");
                        });
            }
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
                                .withStreamPartitioner(
                                        (topic, key, value, numPartitions) -> "bar".equals(value) ? 0 : 1)
                                .withNumberOfPartitions(2));
                repartitioned.to("output");
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
                    TestTopologyFactory.createStreamsTestConfig());
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(
                                new SimpleProducerRecord<>("foo", "bar"),
                                new SimpleProducerRecord<>("foo", "baz")
                        ));
                TestHelper.run(runner);
                KafkaTest.awaitProcessing(executableApp);
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from(new ImprovedStreamsConfig(executableApp.getConfig()).getAppId()
                                      + "-repartition-repartition", POLL_TIMEOUT))
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
                this.softly.assertThat(testClient.read()
                                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                                .from("output", POLL_TIMEOUT))
                        .hasSize(2)
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("bar");
                        })
                        .anySatisfy(outputRecord -> {
                            this.softly.assertThat(outputRecord.key()).isEqualTo("foo");
                            this.softly.assertThat(outputRecord.value()).isEqualTo("baz");
                        });
            }
        }
    }

    @Test
    void shouldUseNumberOfPartitions() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition(
                        RepartitionedX.<String, String>numberOfPartitions(2).withName("repartition"));
                repartitioned.to("output");
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
                    TestTopologyFactory.createStreamsTestConfig());
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
                TestHelper.run(runner);
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
                    final TopicSettings settings = topicClient.describe(appId + "-repartition-repartition");
                    this.softly.assertThat(settings.getPartitions()).isEqualTo(2);
                }
            }
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
        try (final KafkaContainer kafkaCluster = KafkaTest.newCluster()) {
            kafkaCluster.start();
            final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                    .bootstrapServers(kafkaCluster.getBootstrapServers())
                    .build();
            final KafkaTestClient testClient = new KafkaTestClient(endpointConfig);
            testClient.createTopic("input");
            testClient.createTopic("output");
            try (final ConfiguredStreamsApp<StreamsApp> configuredApp = app.configureApp(
                    TestTopologyFactory.createStreamsTestConfig());
                    final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
                    final StreamsRunner runner = executableApp.createRunner()) {
                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
                TestHelper.run(runner);
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
                    final TopicSettings settings = topicClient.describe(appId + "-repartition-repartition");
                    this.softly.assertThat(settings.getPartitions()).isEqualTo(2);
                }
            }
        }
    }
}
