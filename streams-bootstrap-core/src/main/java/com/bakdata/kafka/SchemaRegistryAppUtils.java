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
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class that provides helpers for removing schemas associated with topics
 */
@UtilityClass
@Slf4j
public class SchemaRegistryAppUtils {

    private static final int CACHE_CAPACITY = 100;

    /**
     * Creates a new {@link SchemaRegistryClient} using the specified configuration if
     * {@link AbstractKafkaSchemaSerDeConfig#SCHEMA_REGISTRY_URL_CONFIG} is configured.
     *
     * @param kafkaProperties properties for creating {@link SchemaRegistryClient}
     * @return {@link SchemaRegistryClient} if {@link AbstractKafkaSchemaSerDeConfig#SCHEMA_REGISTRY_URL_CONFIG} is
     * configured
     * @see #createSchemaRegistryClient(Map, String)
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
     * Creates a new {@link SchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to {@link SchemaRegistryClientFactory#newClient(String, int, List, Map, Map)}
     * @param schemaRegistryUrl URL of schema registry
     * @return {@link SchemaRegistryClient}
     */
    public static SchemaRegistryClient createSchemaRegistryClient(@NonNull final Map<String, Object> configs,
            @NonNull final String schemaRegistryUrl) {
        return SchemaRegistryClientFactory.newClient(schemaRegistryUrl, CACHE_CAPACITY, null, configs, null);
    }

    /**
     * Create a hook that cleans up schemas associated with a topic. It is expected that all necessary properties to
     * create a {@link SchemaRegistryClient} are part of {@code kafkaProperties}.
     *
     * @param kafkaProperties Kafka properties to create hook from
     * @return hook that cleans up schemas associated with a topic
     * @see HasTopicHooks#registerTopicHook(TopicHook)
     */
    public static Optional<TopicHook> createTopicHook(final Map<String, Object> kafkaProperties) {
        final Optional<SchemaRegistryClient> schemaRegistryClient =
                createSchemaRegistryClient(kafkaProperties);
        return schemaRegistryClient.map(SchemaRegistryTopicHook::new);
    }

    /**
     * Create a hook that cleans up schemas associated with a topic. It is expected that all necessary properties to
     * create a {@link SchemaRegistryClient} are part of {@link AppConfiguration#getKafkaProperties()}.
     *
     * @param configuration Configuration to create hook from
     * @return hook that cleans up schemas associated with a topic
     * @see #createTopicHook(Map)
     */
    public static Optional<TopicHook> createTopicHook(final AppConfiguration<?> configuration) {
        return createTopicHook(configuration.getKafkaProperties());
    }

    @Slf4j
    @RequiredArgsConstructor
    private static class SchemaRegistryTopicHook implements TopicHook {
        private final @NonNull SchemaRegistryClient schemaRegistryClient;

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
}
