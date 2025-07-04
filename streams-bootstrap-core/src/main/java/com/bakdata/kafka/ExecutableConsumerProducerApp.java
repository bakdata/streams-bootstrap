/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A {@link ProducerApp} with a corresponding {@link ProducerTopicConfig} and Kafka configuration
 * @param <T> type of {@link ProducerApp}
 */
@Builder(access = AccessLevel.PACKAGE)
@Getter
public class ExecutableConsumerProducerApp<T extends ConsumerProducerApp>
        implements ExecutableApp<ConsumerProducerRunner, ConsumerProducerCleanUpRunner, ConsumerProducerExecutionOptions> {
    private final @NonNull StreamsTopicConfig topics;
    private final @NonNull Map<String, Object> producerProperties;
    private final @NonNull Map<String, Object> consumerProperties;
    private final @NonNull T app;
    private final @NonNull String groupId;

    /**
     * Create {@code ProducerCleanUpRunner} in order to clean application
     * @return {@code ProducerCleanUpRunner}
     */
    @Override
    public ConsumerProducerCleanUpRunner createCleanUpRunner() {
        final AppConfiguration<StreamsTopicConfig> configuration = this.createConfiguration();
        final StreamsCleanUpConfiguration streamsCleanUpConfiguration = this.app.setupCleanUp(configuration);
        return ConsumerProducerCleanUpRunner.create(this.topics, this.consumerProperties, this.groupId, streamsCleanUpConfiguration);
    }

    /**
     * Create {@code ProducerRunner} in order to run application
     * @return {@code ProducerRunner}
     */
    @Override
    public ConsumerProducerRunner createRunner() {
        return this.createRunner(ConsumerProducerExecutionOptions.builder().build());
    }

    @Override
    public ConsumerProducerRunner createRunner(final ConsumerProducerExecutionOptions options) {
        final ConsumerBuilder consumerBuilder = new ConsumerBuilder(ConsumerTopicConfig.fromStreamsTopicConfig(this.topics), this.consumerProperties);
        final ProducerBuilder producerBuilder = new ProducerBuilder(ProducerTopicConfig.fromStreamsTopicConfig(this.topics), this.producerProperties);
        final ConsumerProducerBuilder consumerProducerBuilder = new ConsumerProducerBuilder(this.topics, consumerBuilder, producerBuilder);
        final AppConfiguration<StreamsTopicConfig> configuration = this.createConfiguration();
        this.app.setup(configuration);
        return new ConsumerProducerRunner(this.app.buildRunnable(consumerProducerBuilder));
    }

    @Override
    public void close() {
        this.app.close();
    }

    private AppConfiguration<StreamsTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.consumerProperties);
    }

}
