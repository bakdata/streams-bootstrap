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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import picocli.CommandLine;

@Getter
@RequiredArgsConstructor
public final class TestApplicationRunner {

    private final @NonNull String bootstrapServers;
    private final @NonNull SchemaRegistryEnv schemaRegistryEnv;

    private static Thread start(final Runnable runnable) {
        final Thread thread = new Thread(runnable);
        final UncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return thread;
    }

    public Thread start(final KafkaStreamsApplication<? extends StreamsApp> app, final String[] args) {
        final Builder<String> argBuilder = ImmutableList.<String>builder()
                .add(args)
                .add("--bootstrap-servers", this.bootstrapServers);
        if (this.schemaRegistryEnv.getSchemaRegistryUrl() != null) {
            argBuilder.add("--schema-registry-url", this.schemaRegistryEnv.getSchemaRegistryUrl());
        }
        final List<String> newArgs = argBuilder.build();
        return start(() -> KafkaApplication.startApplicationWithoutExit(app, newArgs.toArray(new String[0])));
    }

    public Thread run(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.prepareExecution(app);
        return start(app);
    }

    public void clean(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.prepareExecution(app);
        app.clean();
    }

    public void reset(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.prepareExecution(app);
        app.reset();
    }

    public void prepareExecution(final KafkaStreamsApplication<? extends StreamsApp> app) {
        this.configure(app);
        new CommandLine(app); // initialize all mixins
        app.onApplicationStart();
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

    public KafkaTestClient newTestClient() {
        return new KafkaTestClient(KafkaEndpointConfig.builder()
                .bootstrapServers(this.bootstrapServers)
                .schemaRegistryUrl(this.schemaRegistryEnv.getSchemaRegistryUrl())
                .build());
    }

    public void configure(final KafkaStreamsApplication<? extends StreamsApp> app) {
        app.setBootstrapServers(this.bootstrapServers);
        app.setSchemaRegistryUrl(this.schemaRegistryEnv.getSchemaRegistryUrl());
    }

}
