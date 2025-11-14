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

package com.bakdata.kafka.consumer;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * A {@link ConsumerApp} with a corresponding {@link ConsumerTopicConfig} and Kafka configuration
 *
 * @param <T> type of {@link ConsumerApp}
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class ExecutableConsumerApp<T extends ConsumerApp>
        implements ExecutableApp<ConsumerRunner, ConsumerCleanUpRunner, ConsumerExecutionOptions> {
    private final @NonNull ConsumerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull String groupId;
    private final @NonNull T app;

    /**
     * Create {@code ConsumerCleanUpRunner} in order to clean application
     *
     * @return {@code ConsumerCleanUpRunner}
     */
    @Override
    public ConsumerCleanUpRunner createCleanUpRunner() {
        final AppConfiguration<ConsumerTopicConfig> configuration = this.createConfiguration();
        final ConsumerCleanUpConfiguration configurer = this.app.setupCleanUp(configuration);
        return ConsumerCleanUpRunner.create(this.topics, this.kafkaProperties, this.groupId, configurer);
    }

    /**
     * Create {@code ConsumerRunner} in order to run application
     *
     * @return {@code ConsumerRunner}
     */
    @Override
    public ConsumerRunner createRunner() {
        return this.createRunner(ConsumerExecutionOptions.builder().build());
    }

    @Override
    public ConsumerRunner createRunner(final ConsumerExecutionOptions options) {
        final ConsumerBuilder consumerBuilder = new ConsumerBuilder(this.topics, this.kafkaProperties, options);
        final AppConfiguration<ConsumerTopicConfig> configuration = this.createConfiguration();
        this.app.setup(configuration);
        return new ConsumerRunner(this.app.buildRunnable(consumerBuilder), this.getConfig(), options);
    }

    @Override
    public void close() {
        this.app.close();
    }

    public ConsumerConfig getConfig() {
        return new ConsumerConfig(this.kafkaProperties);
    }

    private AppConfiguration<ConsumerTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.kafkaProperties);
    }
}
