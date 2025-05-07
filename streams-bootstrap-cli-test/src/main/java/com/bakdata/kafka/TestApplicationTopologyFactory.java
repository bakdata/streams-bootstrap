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

import static com.bakdata.kafka.ConfigurationModifiers.configureProperties;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class TestApplicationTopologyFactory {

    private final @NonNull Function<RuntimeConfiguration, RuntimeConfiguration> configurationModifier;

    public TestApplicationTopologyFactory() {
        this(UnaryOperator.identity());
    }

    public static TestApplicationTopologyFactory withSchemaRegistry() {
        return withSchemaRegistry(new TestSchemaRegistry());
    }

    public static TestApplicationTopologyFactory withSchemaRegistry(final TestSchemaRegistry schemaRegistry) {
        return new TestApplicationTopologyFactory(ConfigurationModifiers.withSchemaRegistry(schemaRegistry));
    }

    private static ConfiguredStreamsApp<? extends StreamsApp> createConfiguredApp(
            final KafkaStreamsApplication<? extends StreamsApp> app) {
        app.prepareRun();
        return app.createConfiguredApp();
    }

    public TestApplicationTopologyFactory with(final Map<String, Object> kafkaProperties) {
        return new TestApplicationTopologyFactory(
                this.configurationModifier.andThen(configureProperties(kafkaProperties)));
    }

    public <K, V> TestTopology<K, V> createTopology(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.createTopology(configuredApp);
    }

    public <K, V> TestTopology<K, V> createTopologyExtension(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.createTopologyExtension(configuredApp);
    }

    private TestTopologyFactory createTestTopologyFactory() {
        return new TestTopologyFactory(this.configurationModifier);
    }

}
