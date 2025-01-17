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

import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.kafka.util.TopicSettings;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@RequiredArgsConstructor
public class KafkaTestClient {

    private static final TopicSettings DEFAULT_TOPIC_SETTINGS = TopicSettings.builder()
            .partitions(1)
            .replicationFactor((short) 1)
            .build();
    private final @NonNull KafkaEndpointConfig endpointConfig;

    public static TopicSettings defaultTopicSettings() {
        return DEFAULT_TOPIC_SETTINGS;
    }

    public SenderBuilder send() {
        return new SenderBuilder(this.endpointConfig.createKafkaProperties())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    public ReaderBuilder read() {
        return new ReaderBuilder(this.endpointConfig.createKafkaProperties())
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    public ImprovedAdminClient admin() {
        return ImprovedAdminClient.create(this.endpointConfig.createKafkaProperties());
    }

    public void createTopic(final String topicName, final TopicSettings settings) {
        try (final ImprovedAdminClient admin = this.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.createTopic(topicName, settings);
        }
    }

    public void createTopic(final String topicName) {
        this.createTopic(topicName, defaultTopicSettings());
    }

    public void existsTopic(final String topicName) {
        try (final ImprovedAdminClient admin = this.admin();
                final TopicClient topicClient = admin.getTopicClient()) {
            topicClient.exists(topicName);
        }
    }
}
