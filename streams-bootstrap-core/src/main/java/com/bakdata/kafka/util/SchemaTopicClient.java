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

import com.bakdata.kafka.CleanUpException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * Client to interact with Kafka topics and its associated schema registry subjects in a unified way
 */
@Slf4j
@RequiredArgsConstructor
public final class SchemaTopicClient implements AutoCloseable {
    private static final int CACHE_CAPACITY = 100;
    private final @NonNull TopicClient topicClient;
    private final SchemaRegistryClient schemaRegistryClient;

    /**
     * Creates a new {@code SchemaTopicClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Map)}
     * @param schemaRegistryUrl URL of schema registry
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code SchemaTopicClient}
     */
    public static SchemaTopicClient create(final Map<String, Object> configs, final String schemaRegistryUrl,
            final Duration timeout) {
        final SchemaRegistryClient schemaRegistryClient =
                createSchemaRegistryClient(configs, schemaRegistryUrl);
        final TopicClient topicClient = TopicClient.create(configs, timeout);
        return new SchemaTopicClient(topicClient, schemaRegistryClient);
    }

    /**
     * Creates a new {@code SchemaTopicClient} with no {@link SchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Map)}
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code SchemaTopicClient}
     */
    public static SchemaTopicClient create(final Map<String, Object> configs, final Duration timeout) {
        final TopicClient topicClient = TopicClient.create(configs, timeout);
        return new SchemaTopicClient(topicClient, null);
    }

    /**
     * Creates a new {@link SchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to
     * {@link SchemaRegistryClientFactory#newClient(List, int, List, Map, Map)}
     * @param schemaRegistryUrl URL of schema registry
     * @return {@link SchemaRegistryClient}
     */
    public static SchemaRegistryClient createSchemaRegistryClient(@NonNull final Map<String, Object> configs,
            @NonNull final String schemaRegistryUrl) {
        return SchemaRegistryClientFactory.newClient(List.of(schemaRegistryUrl), CACHE_CAPACITY, null, configs, null);
    }

    /**
     * Delete a topic if it exists and reset the corresponding Schema Registry subjects.
     *
     * @param topic the topic name
     */
    public void deleteTopicAndResetSchemaRegistry(final String topic) {
        this.topicClient.deleteTopicIfExists(topic);
        this.resetSchemaRegistry(topic);
    }

    /**
     * Delete key and value schemas associated with a topic from the schema registry.
     *
     * @param topic the topic name
     */
    public void resetSchemaRegistry(final String topic) {
        if (this.schemaRegistryClient == null) {
            log.debug("No Schema Registry URL set. Skipping schema deletion for topic {}.", topic);
            return;
        }
        log.info("Resetting Schema Registry for topic '{}'", topic);
        try {
            final Collection<String> allSubjects = this.schemaRegistryClient.getAllSubjects();
            final String keySubject = topic + "-key";
            if (allSubjects.contains(keySubject)) {
                this.schemaRegistryClient.deleteSubject(keySubject);
                log.info("Cleaned key schema of topic {}", topic);
            } else {
                log.info("No key schema for topic {} available", topic);
            }
            final String valueSubject = topic + "-value";
            if (allSubjects.contains(valueSubject)) {
                this.schemaRegistryClient.deleteSubject(valueSubject);
                log.info("Cleaned value schema of topic {}", topic);
            } else {
                log.info("No value schema for topic {} available", topic);
            }
        } catch (final IOException | RestClientException e) {
            throw new CleanUpException("Could not reset schema registry for topic " + topic, e);
        }
    }

    @Override
    public void close() {
        this.topicClient.close();
        if (this.schemaRegistryClient != null) {
            try {
                this.schemaRegistryClient.close();
            } catch (final IOException e) {
                throw new UncheckedIOException("Error closing schema registry client", e);
            }
        }
    }
}
