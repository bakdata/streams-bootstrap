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

import static java.util.Collections.emptyMap;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Class that provides helpers for using schema registry in tests.
 */
@RequiredArgsConstructor
@Getter
public final class TestSchemaRegistry {

    private static final String MOCK_URL_PREFIX = "mock://";
    private final @NonNull String schemaRegistryUrl;

    /**
     * Create a new {@code TestSchemaRegistry}. The scope is random in order to avoid collisions between different test
     * instances as scopes are retained globally.
     */
    public TestSchemaRegistry() {
        this(MOCK_URL_PREFIX + UUID.randomUUID());
    }

    /**
     * Get {@link SchemaRegistryClient} for configured URL with default providers
     *
     * @return {@link SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.getSchemaRegistryClient(null);
    }

    /**
     * Get {@link SchemaRegistryClient} for configured URL
     *
     * @param providers list of {@link SchemaProvider} to use for {@link SchemaRegistryClient}
     * @return {@link SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient(final List<SchemaProvider> providers) {
        return SchemaRegistryClientFactory.newClient(this.schemaRegistryUrl, 0, providers, emptyMap(), null);
    }

    /**
     * Configure the schema registry for the provided {@link RuntimeConfiguration}
     *
     * @param configuration {@link RuntimeConfiguration}
     * @return {@link RuntimeConfiguration} with configured
     * {@link AbstractKafkaSchemaSerDeConfig#SCHEMA_REGISTRY_URL_CONFIG}
     */
    public RuntimeConfiguration configure(final RuntimeConfiguration configuration) {
        return configuration.with(this.getKafkaProperties());
    }

    /**
     * Create properties to use schema registry in Kafka apps
     *
     * @return properties to use schema registry in Kafka apps
     */
    public Map<String, String> getKafkaProperties() {
        return Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl
        );
    }
}
