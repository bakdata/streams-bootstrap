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

import static java.util.Collections.emptyMap;

import com.bakdata.kafka.util.AdminClientX;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicSettings;
import com.bakdata.kafka.util.TopicSettings.TopicSettingsBuilder;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;

/**
 * Client that supports communication with Kafka clusters in test setups, including topic management, reading from
 * and sending to topics.
 */
@RequiredArgsConstructor
public class KafkaTestClient {

    private final @NonNull RuntimeConfiguration configuration;

    /**
     * Create a new {@code TopicSettingsBuilder} which uses a single partition and no replicas
     * @return default topic settings
     */
    public static TopicSettingsBuilder defaultTopicSettings() {
        return TopicSettings.builder()
                .partitions(1)
                .replicationFactor((short) 1);
    }

    /**
     * Prepare sending new data to the cluster
     * @return configured {@code SenderBuilder}
     */
    public <K, V> SenderBuilder<K, V> send() {
        return SenderBuilder.create(this.configuration.createKafkaProperties());
    }

    /**
     * Prepare reading data from the cluster. {@link ConsumerConfig#AUTO_OFFSET_RESET_CONFIG} is configured to
     * {@link AutoOffsetResetStrategy#EARLIEST}
     * @return configured {@code ReaderBuilder}
     */
    public <K, V> ReaderBuilder<K, V> read() {
        return ReaderBuilder.<K, V>create(this.configuration.createKafkaProperties())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.type().toString());
    }

    /**
     * Create a new {@link AdminClientX} for the cluster
     * @return configured admin client
     */
    public AdminClientX admin() {
        return AdminClientX.create(this.configuration.createKafkaProperties());
    }

    /**
     * Creates a new Kafka topic with the specified settings.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @param config topic configuration
     */
    public void createTopic(final String topicName, final TopicSettings settings, final Map<String, String> config) {
        try (final AdminClientX admin = this.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.createTopic(topicName, settings, config);
        }
    }

    /**
     * Creates a new Kafka topic with the specified settings. No configs are used.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @see #createTopic(String, TopicSettings, Map)
     */
    public void createTopic(final String topicName, final TopicSettings settings) {
        this.createTopic(topicName, settings, emptyMap());
    }

    /**
     * Creates a new Kafka topic with default settings.
     *
     * @param topicName the topic name
     * @see #createTopic(String, TopicSettings)
     * @see #defaultTopicSettings()
     */
    public void createTopic(final String topicName) {
        this.createTopic(topicName, defaultTopicSettings().build());
    }

    /**
     * Checks whether a Kafka topic exists.
     *
     * @param topicName the topic name
     * @return whether a Kafka topic with the specified name exists or not
     */
    public boolean existsTopic(final String topicName) {
        try (final AdminClientX admin = this.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            return topicClient.exists(topicName);
        }
    }
}
