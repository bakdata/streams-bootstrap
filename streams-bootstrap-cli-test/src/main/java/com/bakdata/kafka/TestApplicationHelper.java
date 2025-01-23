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
import java.lang.Thread.UncaughtExceptionHandler;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import picocli.CommandLine;

@RequiredArgsConstructor
public final class TestApplicationHelper {

    @Getter
    private final @NonNull SchemaRegistryEnv schemaRegistryEnv;

    public Thread runApplication(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.configure(app);
        new CommandLine(app); // initialize all mixins
        app.onApplicationStart();
        final Thread thread = new Thread(app);
        final UncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return thread;
    }

    public ConsumerGroupVerifier verify(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.configure(app);
        final KafkaEndpointConfig endpointConfig = app.getEndpointConfig();
        final KafkaTestClient testClient = new KafkaTestClient(endpointConfig);
        try (final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = app.createConfiguredApp()) {
            final String uniqueAppId = configuredApp.getUniqueAppId();
            return new ConsumerGroupVerifier(uniqueAppId, testClient::admin);
        }
    }

    public ConfiguredStreamsApp<? extends StreamsApp> createConfiguredApp(
            final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.configure(app);
        app.prepareRun();
        return app.createConfiguredApp();
    }

    public <K, V> TestTopology<K, V> createTopology(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = this.createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.createTopology(configuredApp);
    }

    public <K, V> TestTopology<K, V> createTopologyExtension(final KafkaStreamsApplication<? extends StreamsApp> app) {
        final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = this.createConfiguredApp(app);
        final TestTopologyFactory testTopologyFactory = this.createTestTopologyFactory();
        return testTopologyFactory.createTopologyExtension(configuredApp);
    }

    public KafkaTestClient newTestClient(final String bootstrapServers) {
        return new KafkaTestClient(KafkaEndpointConfig.builder()
                .bootstrapServers(bootstrapServers)
                .schemaRegistryUrl(this.schemaRegistryEnv.getSchemaRegistryUrl())
                .build());
    }

    public void configure(final KafkaStreamsApplication<? extends StreamsApp> app) {
        app.setSchemaRegistryUrl(this.schemaRegistryEnv.getSchemaRegistryUrl());
    }

    private TestTopologyFactory createTestTopologyFactory() {
        return new TestTopologyFactory(this.schemaRegistryEnv);
    }

}
