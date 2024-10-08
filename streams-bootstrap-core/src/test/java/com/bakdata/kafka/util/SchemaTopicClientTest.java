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

package com.bakdata.kafka.util;


import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static net.mguenther.kafka.junit.Wait.delay;

import com.bakdata.kafka.TestRecord;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
class SchemaTopicClientTest {
    private static final int TIMEOUT_SECONDS = 10;
    private static final String TOPIC = "topic";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private final EmbeddedKafkaCluster kafkaCluster = newKafkaCluster();

    @InjectSoftAssertions
    SoftAssertions softly;

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
    }

    @AfterEach
    void teardown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldDeleteTopicAndSchemaWhenSchemaRegistryUrlIsSet()
            throws InterruptedException, IOException, RestClientException {
        this.kafkaCluster.createTopic(TopicConfig.withName(TOPIC).useDefaults());
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .as("Topic is created")
                .isTrue();

        final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                .inTransaction(TOPIC, List.of(TestRecord.newBuilder().setContent("foo").build()))
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .build();
        this.kafkaCluster.send(sendRequest);

        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        this.softly.assertThat(client.getAllSubjects())
                .contains(TOPIC + "-value");

        try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
            schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        this.softly.assertThat(client.getAllSubjects())
                .doesNotContain(TOPIC + "-value");
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .isFalse();
    }

    @Test
    void shouldResetSchema() throws InterruptedException, IOException, RestClientException {
        this.kafkaCluster.createTopic(TopicConfig.withName(TOPIC).useDefaults());
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .as("Topic is created")
                .isTrue();

        final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                .inTransaction(TOPIC, List.of(TestRecord.newBuilder().setContent("foo").build()))
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .build();
        this.kafkaCluster.send(sendRequest);

        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        this.softly.assertThat(client.getAllSubjects())
                .contains(TOPIC + "-value");

        try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
            schemaTopicClient.resetSchemaRegistry(TOPIC);
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        this.softly.assertThat(client.getAllSubjects())
                .doesNotContain(TOPIC + "-value");
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .isTrue();
    }

    @Test
    void shouldDeleteTopicAndKeepSchemaWhenSchemaRegistryUrlIsNotSet() throws InterruptedException, RestClientException,
            IOException {
        this.kafkaCluster.createTopic(TopicConfig.withName(TOPIC).useDefaults());
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .as("Topic is created")
                .isTrue();

        final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                .inTransaction(TOPIC, List.of(TestRecord.newBuilder().setContent("foo").build()))
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .build();
        this.kafkaCluster.send(sendRequest);

        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        this.softly.assertThat(client.getAllSubjects())
                .contains(TOPIC + "-value");

        try (final SchemaTopicClient schemaTopicClient = this.createClientWithNoSchemaRegistry()) {
            schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThat(client.getAllSubjects())
                .contains(TOPIC + "-value");
        this.softly.assertThat(this.kafkaCluster.exists(TOPIC))
                .isFalse();
    }

    private SchemaTopicClient createClientWithSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList()
        );
        return SchemaTopicClient.create(kafkaProperties, this.schemaRegistryMockExtension.getUrl(),
                Duration.of(TIMEOUT_SECONDS, ChronoUnit.SECONDS));
    }

    private SchemaTopicClient createClientWithNoSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList()
        );
        return SchemaTopicClient.create(kafkaProperties, Duration.of(TIMEOUT_SECONDS, ChronoUnit.SECONDS));
    }

}
