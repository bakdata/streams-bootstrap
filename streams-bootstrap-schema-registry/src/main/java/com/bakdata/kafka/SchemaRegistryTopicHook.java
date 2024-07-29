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

package com.bakdata.kafka;

import com.bakdata.kafka.HasTopicHooks.TopicHook;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SchemaRegistryTopicHook implements TopicHook {
    private static final int CACHE_CAPACITY = 100;
    private final @NonNull SchemaRegistryClient schemaRegistryClient;

    /**
     * Creates a new {@code SchemaRegistryClient} using the specified configuration if
     * {@link AbstractKafkaSchemaSerDeConfig#SCHEMA_REGISTRY_URL_CONFIG} is configured.
     *
     * @param kafkaProperties properties for creating {@code SchemaRegistryClient}
     * @return {@code SchemaRegistryClient} if {@link AbstractKafkaSchemaSerDeConfig#SCHEMA_REGISTRY_URL_CONFIG} is
     * configured
     * @see SchemaRegistryTopicHook#createSchemaRegistryClient(Map, String)
     */
    public static Optional<SchemaRegistryClient> createSchemaRegistryClient(final Map<String, Object> kafkaProperties) {
        final String schemaRegistryUrl =
                (String) kafkaProperties.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        if (schemaRegistryUrl == null) {
            return Optional.empty();
        }
        final Map<String, Object> properties = new HashMap<>(kafkaProperties);
        properties.remove(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        return Optional.of(createSchemaRegistryClient(properties, schemaRegistryUrl));
    }

    /**
     * Creates a new {@code SchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to
     * {@link SchemaRegistryClientFactory#newClient(List, int, List, Map, Map)}
     * @param schemaRegistryUrl URL of schema registry
     * @return {@code SchemaRegistryClient}
     */
    public static SchemaRegistryClient createSchemaRegistryClient(@NonNull final Map<String, Object> configs,
            @NonNull final String schemaRegistryUrl) {
        return SchemaRegistryClientFactory.newClient(List.of(schemaRegistryUrl), CACHE_CAPACITY, null, configs, null);
    }

    @Override
    public void deleted(final String topic) {
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
        try {
            this.schemaRegistryClient.close();
        } catch (final IOException e) {
            throw new UncheckedIOException("Error closing schema registry client", e);
        }
    }
}
