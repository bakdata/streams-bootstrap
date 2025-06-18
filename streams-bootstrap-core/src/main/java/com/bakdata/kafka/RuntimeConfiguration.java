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

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Runtime configuration to connect to Kafka infrastructure, e.g., bootstrap servers and schema registry.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RuntimeConfiguration {
    static final Set<String> PROVIDED_PROPERTIES = Set.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
    );
    private final @NonNull Map<String, Object> properties;

    /**
     * Create a runtime configuration with the given bootstrap servers.
     *
     * @param bootstrapServers bootstrap servers to connect to
     * @return runtime configuration
     */
    public static RuntimeConfiguration create(final String bootstrapServers) {
        return new RuntimeConfiguration(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    private static void validate(final String key) {
        if (PROVIDED_PROPERTIES.contains(key)) {
            throw new IllegalArgumentException(
                    String.format("Cannot configure '%s'. Please use provided methods", key));
        }
    }

    /**
     * Configure a schema registry for (de-)serialization.
     * @param schemaRegistryUrl schema registry url
     * @return a copy of this runtime configuration with configured schema registry
     */
    public RuntimeConfiguration withSchemaRegistryUrl(final String schemaRegistryUrl) {
        return this.withInternal(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl));
    }

    /**
     * Configure arbitrary Kafka properties
     * @param newProperties properties to configure
     * @return a copy of this runtime configuration with provided properties
     */
    public RuntimeConfiguration with(final Map<String, ?> newProperties) {
        newProperties.keySet().forEach(RuntimeConfiguration::validate);
        return this.withInternal(newProperties);
    }

    /**
     * Configure {@link StreamsConfig#STATE_DIR_CONFIG} for Kafka Streams. Useful for testing
     *
     * @param stateDir directory to use for storing Kafka Streams state
     * @return a copy of this runtime configuration with configured Kafka Streams state directory
     */
    public RuntimeConfiguration withStateDir(final Path stateDir) {
        return this.withInternal(Map.of(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString()));
    }

    /**
     * Disable for Kafka Streams. Useful for testing
     * @return a copy of this runtime configuration with Kafka Streams state store caching disabled
     */
    public RuntimeConfiguration withNoStateStoreCaching() {
        return this.withInternal(Map.of(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, Long.toString(0L)));
    }

    /**
     * Configure {@link ConsumerConfig#SESSION_TIMEOUT_MS_CONFIG} for Kafka consumers. Useful for testing
     * @param sessionTimeout session timeout
     * @return a copy of this runtime configuration with configured consumer session timeout
     */
    public RuntimeConfiguration withSessionTimeout(final Duration sessionTimeout) {
        return this.withInternal(
                Map.of(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Long.toString(sessionTimeout.toMillis())));
    }

    /**
     * Create Kafka properties to connect to infrastructure and modify runtime behavior. {@code bootstrap.servers} is
     * always configured.
     *
     * @return properties used for connecting to Kafka
     */
    public Map<String, Object> createKafkaProperties() {
        return this.properties;
    }

    private RuntimeConfiguration withInternal(final Map<String, ?> newProperties) {
        final Map<String, Object> mergedProperties = new HashMap<>(this.properties);
        mergedProperties.putAll(newProperties);
        return new RuntimeConfiguration(Collections.unmodifiableMap(mergedProperties));
    }

}
