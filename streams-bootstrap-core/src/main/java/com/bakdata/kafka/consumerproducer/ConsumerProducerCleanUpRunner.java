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

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.consumer.ConsumerCleanUpConfiguration;
import com.bakdata.kafka.consumer.ConsumerCleanUpRunner;
import com.bakdata.kafka.consumer.ConsumerTopicConfig;
import com.bakdata.kafka.producer.ProducerCleanUpConfiguration;
import com.bakdata.kafka.producer.ProducerCleanUpRunner;
import com.bakdata.kafka.producer.ProducerTopicConfig;
import com.bakdata.kafka.streams.StreamsCleanUpConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Delete all output topics specified by a {@link StreamsTopicConfig}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConsumerProducerCleanUpRunner implements CleanUpRunner {
    private final @NonNull ConsumerCleanUpRunner consumerCleanUpRunner;
    private final @NonNull ProducerCleanUpRunner producerCleanUpRunner;

    /**
     * Create a new {@code ConsumerProducerCleanUpRunner} with default {@link ConsumerCleanUpConfiguration}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @return {@code ConsumerProducerCleanUpRunner}
     */
    public static ConsumerProducerCleanUpRunner create(@NonNull final StreamsTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId) {
        return create(topics, kafkaProperties, groupId, new StreamsCleanUpConfiguration());
    }

    /**
     * Create a new {@code ConsumerProducerCleanUpRunner}
     *
     * @param topics topic configuration
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @param groupId group id of the consumer
     * @param configuration configuration for hooks that are called when running {@link #clean()}
     * @return {@code ConsumerCleanUpRunner}
     */
    public static ConsumerProducerCleanUpRunner create(@NonNull final StreamsTopicConfig topics,
            @NonNull final Map<String, Object> kafkaProperties,
            @NonNull final String groupId,
            @NonNull final StreamsCleanUpConfiguration configuration) {
        final ConsumerTopicConfig consumerTopicConfig = ConsumerTopicConfig.fromStreamsTopicConfig(topics);
        final ProducerTopicConfig producerTopicConfig = ProducerTopicConfig.fromStreamsTopicConfig(topics);
        final ConsumerCleanUpConfiguration consumerConfig = configuration.toConsumerCleanUpConfiguration();
        final ProducerCleanUpConfiguration producerConfig = configuration.toProducerCleanUpConfiguration();
        final ConsumerCleanUpRunner consumerCleanUpRunner =
                ConsumerCleanUpRunner.create(consumerTopicConfig, kafkaProperties, groupId, consumerConfig);
        final ProducerCleanUpRunner producerCleanUpRunner =
                ProducerCleanUpRunner.create(producerTopicConfig, kafkaProperties, producerConfig);
        return new ConsumerProducerCleanUpRunner(consumerCleanUpRunner, producerCleanUpRunner);
    }

    @Override
    public void close() {
        this.consumerCleanUpRunner.close();
        this.producerCleanUpRunner.close();
    }

    @Override
    public void clean() {
        this.consumerCleanUpRunner.clean();
        this.producerCleanUpRunner.clean();
    }

    /**
     * Reset your ConsumerProducer app by resetting consumer group offsets
     */
    public void reset() {
        this.consumerCleanUpRunner.reset();
    }

}
