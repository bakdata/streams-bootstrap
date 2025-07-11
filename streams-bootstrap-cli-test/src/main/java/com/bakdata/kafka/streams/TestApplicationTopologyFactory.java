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

import static com.bakdata.kafka.streams.ConfigurationModifiers.configureProperties;

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
 * Class that provides helpers for using Fluent Kafka Streams Tests with {@link KafkaStreamsApplication}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestApplicationTopologyFactory {

    private final @NonNull Function<RuntimeConfiguration, RuntimeConfiguration> configurationModifier;

    /**
     * Create a new {@code TestApplicationTopologyFactory}
     */
    public TestApplicationTopologyFactory() {
        this(UnaryOperator.identity());
    }

    /**
     * Create a new {@code TestApplicationTopologyFactory} with configured Schema Registry. The scope is random in order
     * to avoid collisions between different test instances as scopes are retained globally.
     *
     * @return {@code TestTopologyFactory} with configured Schema Registry
     */
    public static TestApplicationTopologyFactory withSchemaRegistry() {
        return withSchemaRegistry(new TestSchemaRegistry());
    }

    /**
     * Create a new {@code TestApplicationTopologyFactory} with configured Schema Registry.
     *
     * @param schemaRegistry Schema Registry to use
     * @return {@code TestApplicationTopologyFactory} with configured Schema Registry
     */
    public static TestApplicationTopologyFactory withSchemaRegistry(final TestSchemaRegistry schemaRegistry) {
        return new TestApplicationTopologyFactory(schemaRegistry::configure);
    }

    private static ConfiguredStreamsApp<? extends StreamsApp> createConfiguredApp(
            final KafkaStreamsApplication<? extends StreamsApp> app) {
        app.prepareRun();
        return app.createConfiguredApp();
    }

    /**
     * Configure arbitrary Kafka properties for the application under test
     *
     * @param kafkaProperties properties to configure
     * @return a copy of this {@code TestApplicationTopologyFactory} with provided properties
     */
    public TestApplicationTopologyFactory with(final Map<String, ?> kafkaProperties) {
        return new TestApplicationTopologyFactory(
                this.configurationModifier.andThen(configureProperties(kafkaProperties)));
    }

    /**
     * Create a {@link TestTopology} from a {@link KafkaStreamsApplication}. It injects a {@link RuntimeConfiguration}
     * for test purposes with Schema Registry optionally configured.
     *
     * @param app KafkaStreamsApplication to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@link TestTopology} that uses topology and configuration provided by {@link KafkaStreamsApplication}
     * @see TestTopologyFactory#createTopology(ConfiguredStreamsApp)
     */
    public <K, V> TestTopology<K, V> createTopology(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.with(app.getKafkaConfig()).createTopology(configuredApp);
    }

    /**
     * Create a {@link TestTopologyExtension} from a {@link KafkaStreamsApplication}. It injects a
     * {@link RuntimeConfiguration} for test purposes with Schema Registry optionally configured.
     *
     * @param app KafkaStreamsApplication to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@link TestTopologyExtension} that uses topology and configuration provided by
     * {@link KafkaStreamsApplication}
     * @see TestTopologyFactory#createTopologyExtension(ConfiguredStreamsApp)
     */
    public <K, V> TestTopologyExtension<K, V> createTopologyExtension(
            final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.createTopologyExtension(configuredApp);
    }

    /**
     * Get Kafka properties from a {@link KafkaStreamsApplication} using a {@link RuntimeConfiguration} for test
     * purposes with Schema Registry optionally configured.
     *
     * @param app KafkaStreamsApplication to get Kafka properties of
     * @return Kafka properties
     * @see TestTopologyFactory#getKafkaProperties(ConfiguredStreamsApp)
     */
    public Map<String, Object> getKafkaProperties(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.getKafkaProperties(configuredApp);
    }

    private TestTopologyFactory createTestTopologyFactory() {
        return new TestTopologyFactory(this.configurationModifier);
    }

}
