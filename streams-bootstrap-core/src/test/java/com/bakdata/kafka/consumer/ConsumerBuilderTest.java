/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

package com.bakdata.kafka.consumer;

import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.Preconfigured;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerBuilderTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    private static ConfiguredConsumerApp<ConsumerApp> createApp(final ConsumerApp app) {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        return new ConfiguredConsumerApp<>(app, new ConsumerAppConfiguration(topics));
    }

    private <K, V> void assertRecords(final List<ConsumerRecord<K, V>> consumedRecords, final KeyValue<K, V> expected) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    final List<KeyValue<K, V>> consumedKeyValues = consumedRecords
                            .stream()
                            .map(TestHelper::toKeyValue)
                            .toList();
                    this.softly.assertThat(consumedKeyValues)
                            .containsExactly(expected);
                });
    }

    @Test
    void shouldCreateConsumerWithDefaultDeserializers() {
        final List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();

        final ConsumerApp app = new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                final Consumer<String, String> consumer = builder.createConsumer();
                builder.subscribeToAllTopics(consumer);
                return builder.createDefaultConsumerRunnable(consumer,
                        records -> records.forEach(consumedRecords::add));
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(StringDeserializer.class, StringDeserializer.class);
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return "test-group";
            }
        };

        try (final ExecutableConsumerApp<ConsumerApp> executableApp = createApp(app)
                .withRuntimeConfiguration(this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {

            runAsync(runner);
            awaitActive(executableApp);

            final KafkaTestClient testClient = this.newTestClient();
            testClient.<String, String>send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
            awaitProcessing(executableApp);

            this.assertRecords(consumedRecords, new KeyValue<>("foo", "bar"));
        }
    }

    @Test
    void shouldCreateConsumerWithStringDeserializers() {
        final List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();

        final ConsumerApp app = new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                final Consumer<String, String> consumer = builder.createConsumer(
                        new StringDeserializer(), new StringDeserializer());
                builder.subscribeToAllTopics(consumer);
                return builder.createDefaultConsumerRunnable(consumer,
                        records -> records.forEach(consumedRecords::add));
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(ByteArrayDeserializer.class, ByteArrayDeserializer.class);
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return "test-group";
            }
        };

        try (final ExecutableConsumerApp<ConsumerApp> executableApp = createApp(app)
                .withRuntimeConfiguration(this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {

            runAsync(runner);
            awaitActive(executableApp);

            final KafkaTestClient testClient = this.newTestClient();
            testClient.<String, String>send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to("input", List.of(new SimpleProducerRecord<>("foo", "bar")));
            awaitProcessing(executableApp);

            this.assertRecords(consumedRecords, new KeyValue<>("foo", "bar"));
        }
    }

    @Test
    void shouldCreateConsumerWithAvroDeserializersRequiringConfiguredSchemaRegistryUrl() {
        final List<ConsumerRecord<TestRecord, TestRecord>> consumedRecords = new ArrayList<>();

        final ConsumerApp app = new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                final Consumer<TestRecord, TestRecord> consumer = builder.createConsumer(
                        new SpecificAvroDeserializer<>(), new SpecificAvroDeserializer<>());
                builder.subscribeToAllTopics(consumer);
                return builder.createDefaultConsumerRunnable(consumer,
                        records -> records.forEach(consumedRecords::add));
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(ByteArrayDeserializer.class, ByteArrayDeserializer.class);
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return "test-group";
            }
        };

        try (final ExecutableConsumerApp<ConsumerApp> executableApp = createApp(app)
                .withRuntimeConfiguration(this.createConfigWithSchemaRegistry());
                final ConsumerRunner runner = executableApp.createRunner()) {

            runAsync(runner);
            awaitActive(executableApp);

            final TestRecord key = TestRecord.newBuilder().setContent("foo").build();
            final TestRecord value = TestRecord.newBuilder().setContent("bar").build();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.<TestRecord, TestRecord>send()
                    .withKeySerializer(new SpecificAvroSerializer<>())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to("input", List.of(new SimpleProducerRecord<>(key, value)));
            awaitProcessing(executableApp);

            this.assertRecords(consumedRecords, new KeyValue<>(key, value));
        }
    }

    @Test
    void shouldCreateConsumerWithPreconfiguredAvroDeserializersRequiringConfiguredSchemaRegistryUrl() {
        final List<ConsumerRecord<TestRecord, TestRecord>> consumedRecords = new ArrayList<>();

        final ConsumerApp app = new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                final Consumer<TestRecord, TestRecord> consumer = builder.createConsumer(
                        Preconfigured.create(new SpecificAvroDeserializer<>()),
                        Preconfigured.create(new SpecificAvroDeserializer<>()));
                builder.subscribeToAllTopics(consumer);
                return builder.createDefaultConsumerRunnable(consumer,
                        records -> records.forEach(consumedRecords::add));
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(ByteArrayDeserializer.class, ByteArrayDeserializer.class);
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return "test-group";
            }
        };

        try (final ExecutableConsumerApp<ConsumerApp> executableApp = createApp(app)
                .withRuntimeConfiguration(this.createConfigWithSchemaRegistry());
                final ConsumerRunner runner = executableApp.createRunner()) {

            runAsync(runner);
            awaitActive(executableApp);

            final TestRecord key = TestRecord.newBuilder().setContent("foo").build();
            final TestRecord value = TestRecord.newBuilder().setContent("bar").build();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.<TestRecord, TestRecord>send()
                    .withKeySerializer(new SpecificAvroSerializer<>())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to("input", List.of(new SimpleProducerRecord<>(key, value)));
            awaitProcessing(executableApp);

            this.assertRecords(consumedRecords, new KeyValue<>(key, value));
        }
    }
}
