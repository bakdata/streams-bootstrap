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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.CommonClientConfigs;

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

    public static RuntimeConfiguration create(final String bootstrapServers) {
        return new RuntimeConfiguration(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    public RuntimeConfiguration withSchemaRegistryUrl(final String schemaRegistryUrl) {
        if (schemaRegistryUrl == null) {
            return this;
        }
        return this.with(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl));
    }

    public RuntimeConfiguration with(final Map<String, ?> newProperties) {
        final Map<String, Object> mergedProperties = new HashMap<>(this.properties);
        mergedProperties.putAll(newProperties);
        return new RuntimeConfiguration(Collections.unmodifiableMap(mergedProperties));
    }

    /**
     * Create Kafka properties to connect to infrastructure. The following properties are configured:
     * <ul>
     *     <li>{@code bootstrap.servers}</li>
     *     <li>{@code schema.registry.url}</li>
     * </ul>
     *
     * @return properties used for connecting to Kafka
     */
    public Map<String, Object> createKafkaProperties() {
        return this.properties;
    }

}
