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

package com.bakdata.kafka.admin;


import static com.bakdata.kafka.KafkaTestClient.defaultTopicSettings;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
class SchemaTopicClientTest extends KafkaTest {
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10);
    private static final String TOPIC = "topic";

    @InjectSoftAssertions
    SoftAssertions softly;

    @Test
    void shouldDeleteTopicAndSchemaWhenSchemaRegistryUrlIsSet()
            throws IOException, RestClientException {
        final KafkaTestClient testClient = this.newTestClient();
        try (final AdminClientX admin = testClient.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.createTopic(TOPIC, defaultTopicSettings().build());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to(TOPIC, List.of(
                            new SimpleProducerRecord<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
                schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
            }

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isFalse();
        }
    }

    @Test
    void shouldResetSchema() throws IOException, RestClientException {
        final KafkaTestClient testClient = this.newTestClient();
        try (final AdminClientX admin = testClient.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.createTopic(TOPIC, defaultTopicSettings().build());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to(TOPIC, List.of(
                            new SimpleProducerRecord<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithSchemaRegistry()) {
                schemaTopicClient.resetSchemaRegistry(TOPIC);
            }

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isTrue();
        }
    }

    @Test
    void shouldDeleteTopicAndKeepSchemaWhenSchemaRegistryUrlIsNotSet() throws RestClientException,
            IOException {
        final KafkaTestClient testClient = this.newTestClient();
        try (final AdminClientX admin = testClient.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.createTopic(TOPIC, defaultTopicSettings().build());
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .as("Topic is created")
                    .isTrue();

            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to(TOPIC, List.of(
                            new SimpleProducerRecord<>(null, TestRecord.newBuilder().setContent("foo").build())
                    ));

            final SchemaRegistryClient client = this.getSchemaRegistryClient();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");

            try (final SchemaTopicClient schemaTopicClient = this.createClientWithNoSchemaRegistry()) {
                schemaTopicClient.deleteTopicAndResetSchemaRegistry(TOPIC);
            }

            this.softly.assertThat(client.getAllSubjects())
                    .contains(TOPIC + "-value");
            this.softly.assertThat(topicClient.exists(TOPIC))
                    .isFalse();
        }
    }

    private SchemaTopicClient createClientWithSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers()
        );
        return SchemaTopicClient.create(kafkaProperties, this.getSchemaRegistryUrl(), CLIENT_TIMEOUT);
    }

    private SchemaTopicClient createClientWithNoSchemaRegistry() {
        final Map<String, Object> kafkaProperties = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers()
        );
        return SchemaTopicClient.create(kafkaProperties, CLIENT_TIMEOUT);
    }

}
