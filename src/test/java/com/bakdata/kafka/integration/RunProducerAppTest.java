/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaProducerApplication;
import com.bakdata.kafka.TestRecord;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RunProducerAppTest {
    private static final int TIMEOUT_SECONDS = 10;
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private final EmbeddedKafkaCluster kafkaCluster = provisionWith(useDefaults());
    private KafkaProducerApplication app = null;

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
    }

    @AfterEach
    void teardown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldRunApp() throws InterruptedException, IOException, RestClientException {
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.forTopic(output).useDefaults());
        this.app = new KafkaProducerApplication() {
            @Override
            protected void runApplication() {
                try (final KafkaProducer<String, TestRecord> producer = this.createProducer()) {
                    final TestRecord record = TestRecord.newBuilder().setContent("bar").build();
                    producer.send(new ProducerRecord<>(this.getOutputTopic(), "foo", record));
                }
            }

            @Override
            protected Properties createKafkaProperties() {
                final Properties kafkaProperties = super.createKafkaProperties();
                kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                return kafkaProperties;
            }
        };
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        this.app.setOutputTopic(output);
        this.app.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, TestRecord.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class)
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .build()))
                .hasSize(1)
                .anySatisfy(kv -> {
                    assertThat(kv.getKey()).isEqualTo("foo");
                    assertThat(kv.getValue().getContent()).isEqualTo("bar");
                });
        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();

        assertThat(client.getAllSubjects())
                .contains(this.app.getOutputTopic() + "-value");
        this.app.setCleanUp(true);
        this.app.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(client.getAllSubjects())
                .doesNotContain(this.app.getOutputTopic() + "-value");
        assertThat(this.kafkaCluster.exists(this.app.getOutputTopic()))
                .as("Output topic is deleted")
                .isFalse();
    }
}
