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

import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaContainerHelper;
import com.bakdata.kafka.KafkaProducerApplication;
import com.bakdata.kafka.ProducerApp;
import com.bakdata.kafka.ProducerBuilder;
import com.bakdata.kafka.ProducerRunnable;
import com.bakdata.kafka.SerializerConfig;
import com.bakdata.kafka.SimpleKafkaProducerApplication;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class RunProducerAppTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    @Container
    private final KafkaContainer kafkaCluster = newKafkaCluster();

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
        try (final KafkaProducerApplication<?> app = new SimpleKafkaProducerApplication<>(() -> new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<String, TestRecord> producer = builder.createProducer()) {
                        final TestRecord testRecord = TestRecord.newBuilder().setContent("bar").build();
                        producer.send(new ProducerRecord<>(builder.getTopics().getOutputTopic(), "foo", testRecord));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
            }
        })) {
            app.setBootstrapServers(this.kafkaCluster.getBootstrapServers());
            app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
            app.setOutputTopic(output);
            app.run();
            final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
            assertThat(kafkaContainerHelper.read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class)
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .<String, TestRecord>from(output, TIMEOUT))
                    .hasSize(1)
                    .anySatisfy(kv -> {
                        assertThat(kv.key()).isEqualTo("foo");
                        assertThat(kv.value().getContent()).isEqualTo("bar");
                    });
            app.clean();
            Thread.sleep(TIMEOUT.toMillis());
            try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
                assertThat(admin.getTopicClient().exists(app.getOutputTopic()))
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }
}
