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

import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicSettings;
import com.bakdata.kafka.util.TopicSettings.TopicSettingsBuilder;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Client that supports communication with Kafka clusters in test setups, including topic management, reading from
 * and sending to topics.
 */
@RequiredArgsConstructor
public class KafkaTestClient {

    private static final TopicSettingsBuilder DEFAULT_TOPIC_SETTINGS = TopicSettings.builder()
            .partitions(1)
            .replicationFactor((short) 1);
    private final @NonNull KafkaEndpointConfig endpointConfig;

    /**
     * Create q new {@code TopicSettingsBuilder} which uses a single partition and no replicas
     * @return default topic settings
     */
    public static TopicSettingsBuilder defaultTopicSettings() {
        return DEFAULT_TOPIC_SETTINGS;
    }

    /**
     * Prepare sending new data to the cluster. {@link StringSerializer} is configured by default.
     * @return configured {@code SenderBuilder}
     */
    public SenderBuilder send() {
        return new SenderBuilder(this.endpointConfig.createKafkaProperties())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    /**
     * Prepare reading data from the cluster. {@link StringDeserializer} is configured by default.
     * @return configured {@code ReaderBuilder}
     */
    public ReaderBuilder read() {
        return new ReaderBuilder(this.endpointConfig.createKafkaProperties())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    /**
     * Create a new {@code ImprovedAdminClient} for the cluster
     * @return configured admin client
     */
    public ImprovedAdminClient admin() {
        return ImprovedAdminClient.create(this.endpointConfig.createKafkaProperties());
    }

    /**
     * Creates a new Kafka topic with the specified settings.
     *
     * @param topicName the topic name
     * @param settings settings for number of partitions and replicationFactor
     * @param config topic configuration
     */
    public void createTopic(final String topicName, final TopicSettings settings, final Map<String, String> config) {
        try (final ImprovedAdminClient admin = this.admin();
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
        try (final ImprovedAdminClient admin = this.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            return topicClient.exists(topicName);
        }
    }
}
