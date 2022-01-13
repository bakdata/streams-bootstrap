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

package com.bakdata.kafka.util;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

@Slf4j
@RequiredArgsConstructor
public final class SchemaTopicClient implements Closeable {
    private final @NonNull TopicClient topicClient;
    private final @NonNull SchemaRegistryClient schemaRegistryClient;

    /**
     * Creates a new {@code SchemaTopicClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Properties)}
     * @param schemaRegistryUrl URL of schema registry
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code SchemaTopicClient}
     */
    public static SchemaTopicClient create(final Properties configs, final String schemaRegistryUrl,
            final Duration timeout) {
        final SchemaRegistryClient schemaRegistryClient =
                createSchemaRegistryClient(configs, schemaRegistryUrl);
        final TopicClient topicClient = TopicClient.create(configs, timeout);
        return new SchemaTopicClient(topicClient, schemaRegistryClient);
    }

    /**
     * Creates a new {@link CachedSchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to {@link CachedSchemaRegistryClient#CachedSchemaRegistryClient(String, int,
     * Map)}
     * @param schemaRegistryUrl URL of schema registry
     * @return {@link CachedSchemaRegistryClient}
     */
    public static CachedSchemaRegistryClient createSchemaRegistryClient(@NonNull final Map<Object, Object> configs,
            @NonNull final String schemaRegistryUrl) {
        final Map<String, Object> originals = new HashMap<>();
        configs.forEach((key, value) -> originals.put(key.toString(), value));
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 100, originals);
    }

    /**
     * Delete a topic if it exists and reset the corresponding schema registry subjects.
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
        log.info("Resetting schema registry for topic '{}'", topic);
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
            throw new RuntimeException("Could not reset schema registry for topic " + topic, e);
        }
    }

    @Override
    public void close() {
        this.topicClient.close();
    }
}
