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

import static com.bakdata.kafka.TestApplicationTopologyFactoryTest.INPUT_TOPIC;
import static com.bakdata.kafka.TestApplicationTopologyFactoryTest.OUTPUT_TOPIC;
import static com.bakdata.kafka.TestApplicationTopologyFactoryTest.createApp;

import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class TestApplicationRunnerTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    private static KafkaStreamsApplication<SimpleStreamsApp> newApp() {
        return TestApplicationTopologyFactoryTest.newApp(new SimpleStreamsApp());
    }

    @Test
    void shouldRun() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = createApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
    }

    @Test
    void shouldRunAsCli() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
    }

    @Test
    void shouldRunUsingSchemaRegistry() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withSchemaRegistry()
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        final SimpleStreamsApp streamsApp = new SimpleStreamsApp() {
            @Override
            public SerdeConfig defaultSerializationConfig() {
                return super.defaultSerializationConfig()
                        .withValueSerde(SpecificAvroSerde.class);
            }
        };
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = createApp(streamsApp)) {
            final TestRecord value = TestRecord.newBuilder()
                    .setContent("content")
                    .build();
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", value)
                    ));
            runner.run(app);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, TestRecord>> records = testClient.<String, TestRecord>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo(value);
                    });
        }
    }

    @Test
    void shouldClean() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = createApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
            app.stop();
            awaitClosed(verifier);
            runner.clean(app);
            this.softly.assertThat(testClient.existsTopic(OUTPUT_TOPIC)).isFalse();
        }
    }

    @Test
    void shouldCleanAsCli() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
            app.stop();
            awaitClosed(verifier);
        }
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            this.softly.assertThat(runner.clean(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC))
                    .isEqualTo(0);
            this.softly.assertThat(testClient.existsTopic(OUTPUT_TOPIC)).isFalse();
        }
    }

    @Test
    void shouldReset() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = createApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records1 = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records1)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
            app.stop();
            awaitClosed(verifier);
            runner.reset(app);
            runner.run(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records2 = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records2)
                    .hasSize(2)
                    .allSatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
    }

    @Test
    void shouldResetAsCli() {
        final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final KafkaTestClient testClient = runner.newTestClient();
        testClient.createTopic(INPUT_TOPIC);
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(INPUT_TOPIC, List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runner.run(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records1 = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records1)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
            app.stop();
            awaitClosed(verifier);
        }
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            this.softly.assertThat(runner.reset(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC))
                    .isEqualTo(0);
        }
        try (final KafkaStreamsApplication<SimpleStreamsApp> app = newApp()) {
            runner.run(app, "--input-topics", INPUT_TOPIC, "--output-topic", OUTPUT_TOPIC);
            final ConsumerGroupVerifier verifier = runner.verify(app);
            awaitProcessing(verifier);
            final List<ConsumerRecord<String, String>> records2 = testClient.<String, String>read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(OUTPUT_TOPIC, POLL_TIMEOUT);
            this.softly.assertThat(records2)
                    .hasSize(2)
                    .allSatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
    }

}
