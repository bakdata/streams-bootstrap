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
import com.bakdata.kafka.Configurator;
import java.util.Map;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Provides all runtime configurations when running a {@link ConsumerApp}
 *
 * @see ConsumerApp#buildRunnable(ConsumerBuilder)
 */
public record ConsumerBuilder(@NonNull ConsumerTopicConfig topics, @NonNull Map<String, Object> kafkaProperties,
                              @NonNull ConsumerExecutionOptions executionOptions) {

    /**
     * Create a new {@code Consumer} using {@link #kafkaProperties}
     *
     * @param <K> type of keys
     * @param <V> type of values
     * @return {@code Consumer}
     * @see KafkaConsumer#KafkaConsumer(Map)
     */
    public <K, V> Consumer<K, V> createConsumer() {
        return new KafkaConsumer<>(this.kafkaProperties);
    }

    /**
     * Create a new {@code Consumer} using {@link #kafkaProperties} and provided {@code Serializers}
     *
     * @param keyDeserializer {@code Serializer} to use for keys
     * @param valueDeserializer {@code Serializer} to use for values
     * @param <K> type of keys
     * @param <V> type of values
     * @return {@code Consumer}
     * @see KafkaConsumer#KafkaConsumer(Map, Deserializer, Deserializer)
     */
    public <K, V> Consumer<K, V> createConsumer(final Deserializer<K> keyDeserializer,
            final Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<>(this.kafkaProperties, keyDeserializer, valueDeserializer);
    }

    /**
     * Create {@code Configurator} to configure {@link Serde} and {@link Serializer} using {@link #kafkaProperties}.
     *
     * @return {@code Configurator}
     */
    public Configurator createConfigurator() {
        return new Configurator(this.kafkaProperties);
    }

    /**
     * Create {@link AppConfiguration} used by this app
     *
     * @return {@link AppConfiguration}
     */
    public AppConfiguration<ConsumerTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.kafkaProperties);
    }

    /**
     * Subscribes the given {@link Consumer} to all input topics and patterns
     * configured in {@link #topics}.
     * This includes all topics from {@code getInputTopics()}, {@code getLabeledInputTopics()},
     * {@code getInputPattern()}, and {@code getLabeledInputPatterns()}.
     *
     * @param <K> type of keys
     * @param <V> type of values
     * @param consumer {@link Consumer} instance to subscribe
     */
    public <K, V> void subscribeToAllTopics(final Consumer<K, V> consumer) {
        if (!this.topics.getInputTopics().isEmpty()) {
            consumer.subscribe(this.topics.getInputTopics());
        }
        if (!this.topics.getLabeledInputTopics().isEmpty()) {
            this.topics.getLabeledInputTopics().values().forEach(consumer::subscribe);
        }
        if (this.topics.getInputPattern() != null) {
            consumer.subscribe(this.topics.getInputPattern());
        }
        if (!this.topics.getLabeledInputPatterns().isEmpty()) {
            this.topics.getLabeledInputPatterns().values().forEach(consumer::subscribe);
        }
    }

    /**
     * Creates a {@link DefaultConsumerRunnable} using the provided consumer, processor,
     * and {@link ConsumerExecutionOptions}.
     *
     * @param <K> type of keys
     * @param <V> type of values
     * @param consumer {@link Consumer} to be used by the runnable
     * @param recordProcessor {@link java.util.function.Consumer} to process {@link ConsumerRecords}
     * @return A new {@link DefaultConsumerRunnable} instance
     */
    public <K, V> DefaultConsumerRunnable<K, V> createDefaultConsumerRunnable(final Consumer<K, V> consumer,
            final java.util.function.Consumer<ConsumerRecords<K, V>> recordProcessor) {
        return new DefaultConsumerRunnable<>(consumer, this.executionOptions, recordProcessor);
    }
}
