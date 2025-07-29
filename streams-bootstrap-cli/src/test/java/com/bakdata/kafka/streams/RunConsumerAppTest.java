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

package com.bakdata.kafka.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.consumer.ConsumerApp;
import com.bakdata.kafka.consumer.ConsumerBuilder;
import com.bakdata.kafka.consumer.ConsumerRunnable;
import com.bakdata.kafka.consumer.ConsumerTopicConfig;
import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.KafkaConsumerApplication;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.SimpleKafkaConsumerApplication;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class RunConsumerAppTest extends KafkaTest {

    @Test
    void shouldRunApp() {
        final String input = "input";
        final List<ConsumerRecord<String, TestRecord>> consumedRecords = new ArrayList<>();
        try (final KafkaConsumerApplication<?> app = new SimpleKafkaConsumerApplication<>(() -> new ConsumerApp() {
            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(StringDeserializer.class, SpecificAvroDeserializer.class);
            }

            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                return () -> {
                    try (final Consumer<String, TestRecord> consumer = builder.createConsumer()) {
                        this.initConsumer(consumer);
                    }
                };
            }

            private void initConsumer(final Consumer<String, TestRecord> consumer) {
                consumer.subscribe(List.of(input));
                while (!Thread.currentThread().isInterrupted()) {
                    final ConsumerRecords<String, TestRecord> consumerRecords = consumer.poll(Duration.ofMillis(100L));
                    consumerRecords.forEach(consumedRecords::add);
                }
            }

            @Override
            public String getUniqueAppId(final ConsumerTopicConfig topics) {
                return "app-id";
            }
        })) {
            app.setBootstrapServers(this.getBootstrapServers());
            final String schemaRegistryUrl = this.getSchemaRegistryUrl();
            app.setSchemaRegistryUrl(schemaRegistryUrl);
            app.setInputTopics(List.of(input));

            new Thread(app).start();
            awaitActive(app.createExecutableApp());

            final TestRecord testRecord = TestRecord.newBuilder().setContent("bar").build();
            final SimpleProducerRecord<String, TestRecord> simpleProducerRecord =
                    new SimpleProducerRecord<>("foo", testRecord);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                    .to(input, List.of(simpleProducerRecord));

            assertThat(consumedRecords).hasSize(1)
                    .anySatisfy(consumerRecord -> {
                        assertThat(consumerRecord.key()).isEqualTo("foo");
                        assertThat(consumerRecord.value().getContent()).isEqualTo("bar");
                    });
        }
    }
}
