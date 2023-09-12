/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * Client to interact with topics associated Schema Registry subjects in a unified way
 */
@Slf4j
@RequiredArgsConstructor
public final class SchemaClient {
    private static final int CACHE_CAPACITY = 100;
    private final @NonNull SchemaRegistryClient schemaRegistryClient;

    /**
     * Creates a new {@code SchemaClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Properties)}
     * @param schemaRegistryUrl URL of Schema Registry
     * @return {@code SchemaClient}
     */
    public static SchemaClient create(final Properties configs, final String schemaRegistryUrl) {
        final SchemaRegistryClient schemaRegistryClient =
                createSchemaRegistryClient(configs, schemaRegistryUrl);
        return new SchemaClient(schemaRegistryClient);
    }

    /**
     * Creates a new {@link CachedSchemaRegistryClient} using the specified configuration.
     *
     * @param configs properties passed to
     * {@link CachedSchemaRegistryClient#CachedSchemaRegistryClient(String, int, Map)}
     * @param schemaRegistryUrl URL of Schema Registry
     * @return {@link CachedSchemaRegistryClient}
     */
    public static CachedSchemaRegistryClient createSchemaRegistryClient(@NonNull final Map<Object, Object> configs,
            @NonNull final String schemaRegistryUrl) {
        final Map<String, Object> originals = new HashMap<>();
        configs.forEach((key, value) -> originals.put(key.toString(), value));
        return new CachedSchemaRegistryClient(schemaRegistryUrl, CACHE_CAPACITY, originals);
    }

    /**
     * Delete key and value schemas associated with a topic from the Schema Registry.
     *
     * @param topic the topic name
     */
    public void resetSchemaRegistry(final String topic) {
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
            throw new CleanUpException("Could not reset Schema Registry for topic " + topic, e);
        }
    }
}
