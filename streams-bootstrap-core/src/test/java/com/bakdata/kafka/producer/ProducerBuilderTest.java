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

package com.bakdata.kafka.producer;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.Preconfigured;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ProducerBuilderTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    private static ConfiguredProducerApp<ProducerApp> createApp(final ProducerApp app) {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        return new ConfiguredProducerApp<>(app, new ProducerAppConfiguration(topics));
    }

    private List<KeyValue<String, String>> readOutputTopic() {
        final List<ConsumerRecord<String, String>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .from("output", POLL_TIMEOUT);

        return records.stream()
                .map(TestHelper::toKeyValue)
                .toList();
    }

    private List<KeyValue<TestRecord, TestRecord>> readOutputTopicAvro() {
        final List<ConsumerRecord<TestRecord, TestRecord>> records = this.newTestClient().read()
                .withKeyDeserializer(new SpecificAvroDeserializer<TestRecord>())
                .withValueDeserializer(new SpecificAvroDeserializer<TestRecord>())
                .from("output", POLL_TIMEOUT);

        return records.stream()
                .map(TestHelper::toKeyValue)
                .toList();
    }

    @Test
    void shouldCreateProducerWithDefaultSerializers() {
        final ProducerApp app = new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<String, String> producer = builder.createProducer()) {
                        producer.send(new ProducerRecord<>("output", "foo", "bar"));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        };

        try (final ProducerRunner runner = createApp(app)
                .withRuntimeConfiguration(this.createConfig())
                .createRunner()) {
            runner.run();

            final List<KeyValue<String, String>> output = this.readOutputTopic();
            this.softly.assertThat(output)
                    .containsExactly(new KeyValue<>("foo", "bar"));
        }
    }

    @Test
    void shouldCreateProducerWithStringSerializers() {
        final ProducerApp app = new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<String, String> producer = builder.createProducer(
                            new StringSerializer(), new StringSerializer())) {
                        producer.send(new ProducerRecord<>("output", "foo", "bar"));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                // Important: Do not set StringSerializer as default to test that serializers provided in
                // createProducer are used
                return new SerializerConfig(ByteArraySerializer.class, ByteArraySerializer.class);
            }
        };

        try (final ProducerRunner runner = createApp(app)
                .withRuntimeConfiguration(this.createConfig())
                .createRunner()) {
            runner.run();

            final List<KeyValue<String, String>> output = this.readOutputTopic();
            this.softly.assertThat(output)
                    .containsExactly(new KeyValue<>("foo", "bar"));
        }
    }

    @Test
    void shouldCreateProducerWithAvroSerializersRequiringConfiguredSchemaRegistryUrl() {
        final ProducerApp app = new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<TestRecord, TestRecord> producer = builder.createProducer(
                            new SpecificAvroSerializer<>(), new SpecificAvroSerializer<>())) {
                        producer.send(new ProducerRecord<>("output",
                                TestRecord.newBuilder().setContent("foo").build(),
                                TestRecord.newBuilder().setContent("bar").build()));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                // Important: Do not set SpecificAvroSerializer as default so that schema.registry.url is not yet
                // configured
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        };

        try (final ProducerRunner runner = createApp(app)
                .withRuntimeConfiguration(this.createConfigWithSchemaRegistry())
                .createRunner()) {
            runner.run();

            final List<KeyValue<TestRecord, TestRecord>> output = this.readOutputTopicAvro();
            this.softly.assertThat(output)
                    .containsExactly(new KeyValue<>(
                            TestRecord.newBuilder().setContent("foo").build(),
                            TestRecord.newBuilder().setContent("bar").build()));
        }
    }

    @Test
    void shouldCreateProducerWithPreconfiguredAvroSerializersRequiringConfiguredSchemaRegistryUrl() {
        final ProducerApp app = new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<TestRecord, TestRecord> producer = builder.createProducer(
                            Preconfigured.create(new SpecificAvroSerializer<>()),
                            Preconfigured.create(new SpecificAvroSerializer<>()))) {
                        producer.send(new ProducerRecord<>("output",
                                TestRecord.newBuilder().setContent("foo").build(),
                                TestRecord.newBuilder().setContent("bar").build()));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                // Important: Do not set SpecificAvroSerializer as default so that schema.registry.url is not yet
                // configured
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        };

        try (final ProducerRunner runner = createApp(app)
                .withRuntimeConfiguration(this.createConfigWithSchemaRegistry())
                .createRunner()) {
            runner.run();

            final List<KeyValue<TestRecord, TestRecord>> output = this.readOutputTopicAvro();
            this.softly.assertThat(output)
                    .containsExactly(new KeyValue<>(
                            TestRecord.newBuilder().setContent("foo").build(),
                            TestRecord.newBuilder().setContent("bar").build()));
        }
    }
}
