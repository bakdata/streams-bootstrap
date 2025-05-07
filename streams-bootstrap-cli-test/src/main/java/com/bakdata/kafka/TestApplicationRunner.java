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

import static com.bakdata.kafka.AsyncRunnable.runAsync;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TestApplicationRunner {

    private final @NonNull String bootstrapServers;
    private final TestSchemaRegistry schemaRegistry;
    private final @NonNull Map<String, String> kafkaConfig;

    public static TestApplicationRunner create(final String bootstrapServers) {
        return new TestApplicationRunner(bootstrapServers, null, emptyMap());
    }

    private static Map<String, String> merge(final Map<String, String> map1, final Map<String, String> map2) {
        final Map<String, String> merged = new HashMap<>(map1);
        merged.putAll(map2);
        return Collections.unmodifiableMap(merged);
    }

    public TestApplicationRunner withStateDir(final Path stateDir) {
        return this.withKafkaConfig(Map.of(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString()));
    }

    public TestApplicationRunner withTestConfig() {
        return this.withKafkaConfig(TestConfigurator.createStreamsTestConfig());
    }

    public TestApplicationRunner withKafkaConfig(final Map<String, String> newKafkaConfig) {
        return new TestApplicationRunner(
                this.bootstrapServers, this.schemaRegistry, merge(this.kafkaConfig, newKafkaConfig));
    }

    public TestApplicationRunner withSchemaRegistry(final @NonNull TestSchemaRegistry schemaRegistry) {
        return new TestApplicationRunner(this.bootstrapServers, schemaRegistry, this.kafkaConfig);
    }

    public TestApplicationRunner withSchemaRegistry() {
        return this.withSchemaRegistry(new TestSchemaRegistry());
    }

    public void run(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app, final String[] args) {
        final String[] newArgs = this.setupArgs(args, emptyList());
        final Thread thread = new Thread(() -> KafkaApplication.startApplicationWithoutExit(app, newArgs));
        thread.start();
    }

    public int clean(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app, final String[] args) {
        final String[] newArgs = this.setupArgs(args, List.of("clean"));
        return KafkaApplication.startApplicationWithoutExit(app, newArgs);
    }

    public int reset(final KafkaStreamsApplication<? extends StreamsApp> app, final String[] args) {
        final String[] newArgs = this.setupArgs(args, List.of("reset"));
        return KafkaApplication.startApplicationWithoutExit(app, newArgs);
    }

    public AsyncRunnable run(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app) {
        this.prepareExecution(app);
        return runAsync(app);
    }

    public void clean(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app) {
        this.prepareExecution(app);
        app.clean();
    }

    public void reset(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.prepareExecution(app);
        app.reset();
    }

    public void prepareExecution(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app) {
        this.configure(app);
        app.onApplicationStart();
    }

    public ConsumerGroupVerifier verify(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.configure(app);
        final RuntimeConfiguration configuration = app.getRuntimeConfiguration();
        final KafkaTestClient testClient = new KafkaTestClient(configuration);
        try (final ConfiguredStreamsApp<? extends StreamsApp> configuredApp = app.createConfiguredApp()) {
            final String uniqueAppId = configuredApp.getUniqueAppId();
            return new ConsumerGroupVerifier(uniqueAppId, testClient::admin);
        }
    }

    public KafkaTestClient newTestClient() {
        final RuntimeConfiguration configuration = RuntimeConfiguration.create(this.bootstrapServers)
                .withSchemaRegistryUrl(this.schemaRegistry != null ? this.schemaRegistry.getSchemaRegistryUrl() : null)
                .with(this.kafkaConfig);
        return new KafkaTestClient(configuration);
    }

    public void configure(final KafkaApplication<?, ?, ?, ?, ?, ?, ?> app) {
        app.setBootstrapServers(this.bootstrapServers);
        final Map<String, String> mergedConfig = merge(app.getKafkaConfig(), this.kafkaConfig);
        app.setKafkaConfig(mergedConfig);
        if (this.schemaRegistry != null) {
            app.setSchemaRegistryUrl(this.schemaRegistry.getSchemaRegistryUrl());
        }
    }

    private String[] setupArgs(final String[] args, final Iterable<String> command) {
        final ImmutableList.Builder<String> argBuilder = ImmutableList.<String>builder()
                .add(args)
                .add("--bootstrap-servers", this.bootstrapServers);
        if (this.schemaRegistry != null) {
            argBuilder.add("--schema-registry-url", this.schemaRegistry.getSchemaRegistryUrl());
        }
        this.kafkaConfig.forEach((k, v) -> argBuilder.add("--kafka-config", String.format("%s=%s", k, v)));
        final List<String> newArgs = argBuilder
                .addAll(command)
                .build();
        return newArgs.toArray(new String[0]);
    }

}
