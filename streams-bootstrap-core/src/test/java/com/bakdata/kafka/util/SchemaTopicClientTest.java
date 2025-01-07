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

package com.bakdata.kafka.util;


import static com.bakdata.kafka.KafkaContainerHelper.DEFAULT_TOPIC_SETTINGS;
import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static java.util.Collections.emptyMap;

import com.bakdata.kafka.KafkaContainerHelper;
import com.bakdata.kafka.TestRecord;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
class SchemaTopicClientTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final String TOPIC = "topic";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    @Container
    private final KafkaContainer kafkaCluster = newKafkaCluster();

    @InjectSoftAssertions
    SoftAssertions softly;

    @Test
    void shouldDeleteTopicAndSchemaWhenSchemaRegistryUrlIsSet()
            throws InterruptedException, IOException, RestClientException {
        final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
        try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
            final TopicClient topicClient = admin.getTopicClient();
            topicClient.createTopic(TOPIC, DEFAULT_TOPIC_SETTINGS, emptyMap());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            kafkaContainerHelper.send()
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .to(TOPIC, List.of(
                            new KeyValue<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
                schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
            }

            Thread.sleep(TIMEOUT.toMillis());

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isFalse();
        }
    }

    @Test
    void shouldResetSchema() throws InterruptedException, IOException, RestClientException {
        final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
        try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
            final TopicClient topicClient = admin.getTopicClient();
            topicClient.createTopic(TOPIC, DEFAULT_TOPIC_SETTINGS, emptyMap());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            kafkaContainerHelper.send()
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .to(TOPIC, List.of(
                            new KeyValue<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
                schemaTopicClient.resetSchemaRegistry(TOPIC);
            }

            Thread.sleep(TIMEOUT.toMillis());

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isTrue();
        }
    }

    @Test
    void shouldDeleteTopicAndKeepSchemaWhenSchemaRegistryUrlIsNotSet() throws InterruptedException, RestClientException,
            IOException {
        final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
        try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
            final TopicClient topicClient = admin.getTopicClient();
            topicClient.createTopic(TOPIC, DEFAULT_TOPIC_SETTINGS, emptyMap());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            kafkaContainerHelper.send()
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .to(TOPIC, List.of(
                            new KeyValue<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithNoSchemaRegistry()) {
                schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
            }

            Thread.sleep(TIMEOUT.toMillis());
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isFalse();
        }
    }

    private SchemaTopicClient createClientWithSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBootstrapServers()
        );
        return SchemaTopicClient.create(kafkaProperties, this.schemaRegistryMockExtension.getUrl(),
                TIMEOUT);
    }

    private SchemaTopicClient createClientWithNoSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBootstrapServers()
        );
        return SchemaTopicClient.create(kafkaProperties, TIMEOUT);
    }

}
