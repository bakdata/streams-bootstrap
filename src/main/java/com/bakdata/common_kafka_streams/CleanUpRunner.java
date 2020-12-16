/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

package com.bakdata.common_kafka_streams;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.StreamsConfig;

@Slf4j
@RequiredArgsConstructor
@Getter
public abstract class CleanUpRunner {
    private final @NonNull Properties kafkaProperties;
    private final @NonNull SchemaRegistryClient client;
    private final @NonNull String brokers;

    private static CachedSchemaRegistryClient createSchemaRegistryClient(@NonNull final Properties kafkaProperties,
            @NonNull final String schemaRegistryUrl) {
        final StreamsConfig streamsConfig = new StreamsConfig(kafkaProperties);
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 100, streamsConfig.originals());
    }

    protected CleanUpRunner(final @NonNull Properties kafkaProperties, final @NonNull String schemaRegistryUrl,
            final @NonNull String brokers) {
        this.kafkaProperties = kafkaProperties;
        this.client = createSchemaRegistryClient(kafkaProperties, schemaRegistryUrl);
        this.brokers = brokers;
    }

    public void deleteTopicAndResetSchemaRegistry(final String topic) {
        this.deleteTopic(topic);
        this.resetSchemaRegistry(topic);
    }

    public void deleteTopic(final String topic) {
        log.info("Delete topic: {}", topic);
        try (final AdminClient adminClient = this.createAdminClient()) {
            adminClient.deleteTopics(List.of(topic));
        }
    }

    public void resetSchemaRegistry(final String topic) {
        log.info("Reset topic: {}", topic);
        try {
            final Collection<String> allSubjects = this.client.getAllSubjects();
            final String keySubject = topic + "-key";
            if (allSubjects.contains(keySubject)) {
                this.client.deleteSubject(keySubject);
                log.info("Cleaned key schema of topic {}", topic);
            } else {
                log.info("No key schema for topic {} available", topic);
            }
            final String valueSubject = topic + "-value";
            if (allSubjects.contains(valueSubject)) {
                this.client.deleteSubject(valueSubject);
                log.info("Cleaned value schema of topic {}", topic);
            } else {
                log.info("No value schema for topic {} available", topic);
            }
        } catch (final IOException | RestClientException e) {
            throw new RuntimeException("Could not reset schema registry for topic " + topic, e);
        }
    }

    protected AdminClient createAdminClient() {
        return AdminClient.create(this.kafkaProperties);
    }
}
