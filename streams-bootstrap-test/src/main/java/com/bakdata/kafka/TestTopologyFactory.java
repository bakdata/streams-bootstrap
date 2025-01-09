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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Class that provides helpers for using Fluent Kafka Streams Tests with {@link ConfiguredStreamsApp}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestTopologyFactory {

    private static final String MOCK_URL_PREFIX = "mock://";
    private final String schemaRegistryUrl;

    /**
     * Create a new {@code TestTopologyFactory} with no configured Schema Registry.
     * @return {@code TestTopologyFactory} with no configured Schema Registry
     */
    public static TestTopologyFactory withoutSchemaRegistry() {
        return withSchemaRegistry(null);
    }

    /**
     * Create a new {@code TestTopologyFactory} with configured
     * {@link io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}. The scope is random in order to avoid
     * collisions between different test instances as scopes are retained globally.
     * @return {@code TestTopologyFactory} with configured Schema Registry
     */
    public static TestTopologyFactory withSchemaRegistry() {
        return withSchemaRegistry(MOCK_URL_PREFIX + UUID.randomUUID());
    }

    /**
     * Create a new {@code TestTopologyFactory} with configured Schema Registry.
     * @param schemaRegistryUrl Schema Registry URL to use
     * @return {@code TestTopologyFactory} with configured Schema Registry
     */
    public static TestTopologyFactory withSchemaRegistry(final String schemaRegistryUrl) {
        return new TestTopologyFactory(schemaRegistryUrl);
    }

    /**
     * Create {@code Configurator} to configure {@link org.apache.kafka.common.serialization.Serde} and
     * {@link org.apache.kafka.common.serialization.Serializer} using the {@code TestTopology} properties.
     * @param testTopology {@code TestTopology} to use properties of
     * @return {@code Configurator}
     * @see TestTopology#getProperties()
     */
    public static Configurator createConfigurator(final TestTopology<?, ?> testTopology) {
        return new Configurator(testTopology.getProperties());
    }

    /**
     * Get Schema Registry URL if configured
     * @return Schema Registry URL
     * @throws NullPointerException if Schema Registry is not configured
     */
    public String getSchemaRegistryUrl() {
        return Objects.requireNonNull(this.schemaRegistryUrl, "Schema Registry is not configured");
    }

    /**
     * Get {@code SchemaRegistryClient} for configured URL with default providers
     * @return {@code SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.getSchemaRegistryClient(null);
    }

    /**
     * Get {@code SchemaRegistryClient} for configured URL
     * @param providers list of {@code SchemaProvider} to use for {@code SchemaRegistryClient}
     * @return {@code SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient(final List<SchemaProvider> providers) {
        return SchemaRegistryClientFactory.newClient(List.of(this.getSchemaRegistryUrl()), 0, providers, emptyMap(),
                null);
    }

    /**
     * Create a {@code TestTopology} from a {@code ConfiguredStreamsApp}. It injects a {@link KafkaEndpointConfig}
     * for test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopology} that uses topology and configuration provided by {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopology<K, V> createTopology(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopology<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Create a {@code TestTopologyExtension} from a {@code ConfiguredStreamsApp}. It injects a
     * {@link KafkaEndpointConfig} for test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopologyExtension} that uses topology and configuration provided by
     * {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopologyExtension<K, V> createTopologyExtension(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopologyExtension<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Get Kafka properties from a {@code ConfiguredStreamsApp} using a {@link KafkaEndpointConfig} for test purposes
     * with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to get Kafka properties of
     * @return Kafka properties
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     */
    public Map<String, Object> getKafkaProperties(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                .bootstrapServers("localhost:9092")
                .schemaRegistryUrl(this.schemaRegistryUrl)
                .build();
        return app.getKafkaProperties(endpointConfig);
    }
}
