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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.consumer.ConsumerBuilder;
import com.bakdata.kafka.consumer.ConsumerTopicConfig;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.producer.ProducerTopicConfig;
import com.bakdata.kafka.streams.StreamsCleanUpConfiguration;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * A {@link ConsumerProducerApp} with a corresponding {@link ConsumerProducerTopicConfig} and Kafka configuration
 *
 * @param <T> type of {@link ConsumerProducerApp}
 */
@Builder(access = AccessLevel.PACKAGE)
@Getter
public class ExecutableConsumerProducerApp<T extends ConsumerProducerApp>
        implements
        ExecutableApp<ConsumerProducerRunner, ConsumerProducerCleanUpRunner, ConsumerProducerExecutionOptions> {
    private final @NonNull ConsumerProducerTopicConfig topics;
    private final @NonNull Map<String, Object> producerProperties;
    private final @NonNull Map<String, Object> consumerProperties;
    private final @NonNull T app;
    private final @NonNull String groupId;

    /**
     * Create {@code ConsumerProducerCleanUpRunner} in order to clean application
     *
     * @return {@code ConsumerProducerCleanUpRunner}
     */
    @Override
    public ConsumerProducerCleanUpRunner createCleanUpRunner() {
        final AppConfiguration<ConsumerProducerTopicConfig> configuration = this.createConfiguration();
        final StreamsCleanUpConfiguration streamsCleanUpConfiguration = this.app.setupCleanUp(configuration);
        return ConsumerProducerCleanUpRunner.create(this.topics, this.consumerProperties, this.groupId,
                streamsCleanUpConfiguration);
    }

    /**
     * Create {@code ConsumerProducerRunner} in order to run application
     *
     * @return {@code ConsumerProducerRunner}
     */
    @Override
    public ConsumerProducerRunner createRunner() {
        final ConsumerProducerExecutionOptions executionOptions = ConsumerProducerExecutionOptions.builder().build();
        return this.createRunner(executionOptions);
    }

    @Override
    public ConsumerProducerRunner createRunner(final ConsumerProducerExecutionOptions options) {
        final ConsumerBuilder consumerBuilder = new ConsumerBuilder(this.topics.toConsumerTopicConfig(),
                this.consumerProperties, options.toConsumerExecutionOptions());
        final ProducerBuilder producerBuilder = new ProducerBuilder(this.topics.toProducerTopicConfig(),
                this.producerProperties);
        final ConsumerProducerBuilder
                consumerProducerBuilder = new ConsumerProducerBuilder(this.topics, consumerBuilder, producerBuilder);
        final AppConfiguration<ConsumerProducerTopicConfig> configuration = this.createConfiguration();
        this.app.setup(configuration);
        return new ConsumerProducerRunner(this.app.buildRunnable(consumerProducerBuilder),
                this.getConsumerConfig(),
                this.getProducerConfig(),
                options);
    }

    @Override
    public void close() {
        this.app.close();
    }

    public ConsumerConfig getConsumerConfig() {
        return new ConsumerConfig(this.consumerProperties);
    }

    public ProducerConfig getProducerConfig() {
        return new ProducerConfig(this.producerProperties);
    }

    private AppConfiguration<ConsumerProducerTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.consumerProperties);
    }

}
