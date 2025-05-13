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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Class that provides helpers for using Fluent Kafka Streams Tests with {@link ConfiguredStreamsApp}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestTopologyFactory {

    private static final Map<String, String> STREAMS_TEST_CONFIG = Map.of(
            // Disable caching to allow immediate aggregations
            StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, Long.toString(0L),
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(10_000)
    );
    private final String schemaRegistryUrl;
    private final @NonNull Map<String, Object> kafkaProperties;

    /**
     * Create a new {@code TestTopologyFactory} with no configured Schema Registry.
     *
     * @return {@code TestTopologyFactory} with no configured Schema Registry
     */
    public static TestTopologyFactory withoutSchemaRegistry() {
        return new TestTopologyFactory(null);
    }

    /**
     * Create a new {@code TestTopologyFactory} with configured Schema Registry. The scope is random in order to avoid
     * collisions between different test instances as scopes are retained globally.
     *
     * @return {@code TestTopologyFactory} with configured Schema Registry
     */
    public static TestTopologyFactory withSchemaRegistry() {
        return withSchemaRegistry(new TestSchemaRegistry());
    }

    /**
     * Create a new {@code TestTopologyFactory} with configured Schema Registry.
     *
     * @param schemaRegistry Schema Registry to use
     * @return {@code TestTopologyFactory} with configured Schema Registry
     */
    public static TestTopologyFactory withSchemaRegistry(final TestSchemaRegistry schemaRegistry) {
        return new TestTopologyFactory(schemaRegistry.getSchemaRegistryUrl(), emptyMap());
    }

    /**
     * Create a new Kafka Streams config suitable for test environments. This includes setting the following parameters
     * in addition to {@link #createStreamsTestConfig()}:
     * <ul>
     *     <li>{@link StreamsConfig#STATE_DIR_CONFIG}=provided directory</li>
     * </ul>
     *
     * @param stateDir directory to use for storing Kafka Streams state
     * @return Kafka Streams config
     * @see #createStreamsTestConfig()
     */
    public static Map<String, String> createStreamsTestConfig(final Path stateDir) {
        final Map<String, String> config = new HashMap<>(createStreamsTestConfig());
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return Map.copyOf(config);
    }

    /**
     * Create a new Kafka Streams config suitable for test environments. This includes setting the following
     * parameters:
     * <ul>
     *     <li>{@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG}=0</li>
     *     <li>{@link ConsumerConfig#SESSION_TIMEOUT_MS_CONFIG}=10000</li>
     * </ul>
     *
     * @return Kafka Streams config
     */
    public static Map<String, String> createStreamsTestConfig() {
        return STREAMS_TEST_CONFIG;
    }

    /**
     * Configure arbitrary Kafka properties for the application under test
     *
     * @param kafkaProperties properties to configure
     * @return a copy of this {@code TestTopologyFactory} with provided properties
     */
    public TestTopologyFactory with(final Map<String, Object> kafkaProperties) {
        return new TestTopologyFactory(this.schemaRegistryUrl, kafkaProperties);
    }

    /**
     * Create a {@code TestTopology} from a {@code ConfiguredStreamsApp}. It injects a {@link RuntimeConfiguration} for
     * test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopology} that uses topology and configuration provided by {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(RuntimeConfiguration)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopology<K, V> createTopology(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopology<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Create a {@code TestTopologyExtension} from a {@code ConfiguredStreamsApp}. It injects a
     * {@link RuntimeConfiguration} for test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopologyExtension} that uses topology and configuration provided by
     * {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(RuntimeConfiguration)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopologyExtension<K, V> createTopologyExtension(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopologyExtension<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Get Kafka properties from a {@code ConfiguredStreamsApp} using a {@link RuntimeConfiguration} for test purposes
     * with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to get Kafka properties of
     * @return Kafka properties
     * @see ConfiguredStreamsApp#getKafkaProperties(RuntimeConfiguration)
     */
    public Map<String, Object> getKafkaProperties(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        final RuntimeConfiguration configuration = this.createConfiguration();
        return app.getKafkaProperties(configuration);
    }

    private RuntimeConfiguration createConfiguration() {
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092")
                .with(this.kafkaProperties);
        return this.schemaRegistryUrl == null ? configuration
                : configuration.withSchemaRegistryUrl(this.schemaRegistryUrl);
    }
}
