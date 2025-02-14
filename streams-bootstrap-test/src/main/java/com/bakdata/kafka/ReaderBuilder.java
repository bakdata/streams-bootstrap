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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Read data from a Kafka cluster
 */
@RequiredArgsConstructor
public class ReaderBuilder {

    private final @NonNull Map<String, Object> properties;

    private static <K, V> List<ConsumerRecord<K, V>> pollAll(final Consumer<K, V> consumer, final Duration timeout) {
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        ConsumerRecords<K, V> poll;
        do {
            poll = consumer.poll(timeout);
            poll.forEach(records::add);
        } while (!poll.isEmpty());
        return records;
    }

    private static <K, V> List<ConsumerRecord<K, V>> readAll(final Consumer<K, V> consumer, final String topic,
            final Duration timeout) {
        final List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
        final List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(ReaderBuilder::toTopicPartition)
                .collect(Collectors.toList());
        consumer.assign(topicPartitions);
        return pollAll(consumer, timeout);
    }

    private static TopicPartition toTopicPartition(final PartitionInfo partition) {
        return new TopicPartition(partition.topic(), partition.partition());
    }

    /**
     * Add a consumer configuration
     * @param key configuration key
     * @param value configuration value
     * @return {@code ReaderBuilder} with added configuration
     */
    public ReaderBuilder with(final String key, final Object value) {
        final Map<String, Object> newProperties = new HashMap<>(this.properties);
        newProperties.put(key, value);
        return new ReaderBuilder(Map.copyOf(newProperties));
    }

    /**
     * Read all data from a topic. This method is idempotent, meaning calling it multiple times will read the same
     * data unless the data in the topic changes.
     * @param topic topic to read from
     * @param timeout consumer poll timeout
     * @return consumed records
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> List<ConsumerRecord<K, V>> from(final String topic, final Duration timeout) {
        try (final Consumer<K, V> consumer = this.createConsumer()) {
            return readAll(consumer, topic, timeout);
        }
    }

    /**
     * Create a new {@code Consumer} for a Kafka cluster
     * @return {@code Consumer}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public <K, V> Consumer<K, V> createConsumer() {
        return new KafkaConsumer<>(this.properties);
    }

}
