/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaProducerApplication;
import com.bakdata.kafka.ProducerApp;
import com.bakdata.kafka.ProducerBuilder;
import com.bakdata.kafka.ProducerRunnable;
import com.bakdata.kafka.SimpleKafkaProducerApplication;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RunProducerAppTest {
    private static final int TIMEOUT_SECONDS = 10;
    private final EmbeddedKafkaCluster kafkaCluster = provisionWith(defaultClusterConfig());

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
    }

    @AfterEach
    void tearDown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldRunApp() throws InterruptedException {
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
        try (final KafkaProducerApplication app = new SimpleKafkaProducerApplication(() -> new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<String, TestRecord> producer = builder.createProducer()) {
                        final TestRecord record = TestRecord.newBuilder().setContent("bar").build();
                        producer.send(new ProducerRecord<>(builder.getTopics().getOutputTopic(), "foo", record));
                    }
                };
            }

            @Override
            public Map<String, Object> createKafkaProperties() {
                return Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                );
            }
        })) {
            app.setBrokers(this.kafkaCluster.getBrokerList());
            app.setSchemaRegistryUrl("mock://");
            app.setOutputTopic(output);
            app.setKafkaConfig(Map.of(
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
            ));
            app.run();
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, TestRecord.class)
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class)
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://")
                    .build()))
                    .hasSize(1)
                    .anySatisfy(kv -> {
                        assertThat(kv.getKey()).isEqualTo("foo");
                        assertThat(kv.getValue().getContent()).isEqualTo("bar");
                    });
            app.clean();
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(this.kafkaCluster.exists(app.getOutputTopic()))
                    .as("Output topic is deleted")
                    .isFalse();
        }
    }
}