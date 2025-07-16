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

package com.bakdata.kafka.streams;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.TestSchemaRegistry;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Class that provides helpers for using Fluent Kafka Streams Tests with {@link ConfiguredStreamsApp}
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class TestTopologyFactory {

    private final @NonNull Function<RuntimeConfiguration, RuntimeConfiguration> configurationModifier;

    /**
     * Create a new {@code TestTopologyFactory}
     */
    public TestTopologyFactory() {
        this(UnaryOperator.identity());
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
        return new TestTopologyFactory(schemaRegistry::configure);
    }

    /**
     * Configure arbitrary Kafka properties for the application under test
     *
     * @param kafkaProperties properties to configure
     * @return a copy of this {@code TestTopologyFactory} with provided properties
     */
    public TestTopologyFactory with(final Map<String, ?> kafkaProperties) {
        return new TestTopologyFactory(
                this.configurationModifier.andThen(ConfigurationModifiers.configureProperties(kafkaProperties)));
    }

    /**
     * Create a {@link TestTopology} from a {@link ConfiguredStreamsApp}. It injects a {@link RuntimeConfiguration} for
     * test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@link TestTopology} that uses topology and configuration provided by {@link ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(RuntimeConfiguration)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopology<K, V> createTopology(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopology<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Create a {@link TestTopologyExtension} from a {@link ConfiguredStreamsApp}. It injects a
     * {@link RuntimeConfiguration} for test purposes with Schema Registry optionally configured.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@link TestTopologyExtension} that uses topology and configuration provided by
     * {@link ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(RuntimeConfiguration)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public <K, V> TestTopologyExtension<K, V> createTopologyExtension(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopologyExtension<>(app::createTopology, this.getKafkaProperties(app));
    }

    /**
     * Get Kafka properties from a {@link ConfiguredStreamsApp} using a {@link RuntimeConfiguration} for test purposes
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
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        return this.configurationModifier.apply(configuration);
    }
}
