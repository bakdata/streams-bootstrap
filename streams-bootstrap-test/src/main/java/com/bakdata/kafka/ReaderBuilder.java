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
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Read data from a Kafka cluster
 * @param <K> type of keys deserialized by the reader
 * @param <V> type of values deserialized by the reader
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReaderBuilder<K, V> {

    private final @NonNull Map<String, Object> properties;
    private final @NonNull Preconfigured<Deserializer<K>> keyDeserializer;
    private final @NonNull Preconfigured<Deserializer<V>> valueDeserializer;

    static <K, V> ReaderBuilder<K, V> create(final Map<String, Object> properties) {
        return new ReaderBuilder<>(properties, Preconfigured.defaultDeserializer(),
                Preconfigured.defaultDeserializer());
    }

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
        // TODO
        if(partitionInfos == null) return List.of();
        final List<TopicPartition> topicPartitions = partitionInfos.stream()
                .map(ReaderBuilder::toTopicPartition)
                .toList();
        consumer.assign(topicPartitions);
        return pollAll(consumer, timeout);
    }

    private static TopicPartition toTopicPartition(final PartitionInfo partition) {
        return new TopicPartition(partition.topic(), partition.partition());
    }

    /**
     * Add a consumer configuration
     *
     * @param key configuration key
     * @param value configuration value
     * @return {@code ReaderBuilder} with added configuration
     */
    public ReaderBuilder<K, V> with(final String key, final Object value) {
        final Map<String, Object> newProperties = new HashMap<>(this.properties);
        newProperties.put(key, value);
        return new ReaderBuilder<>(Map.copyOf(newProperties), this.keyDeserializer, this.valueDeserializer);
    }

    /**
     * Read all data from a topic. This method is idempotent, meaning calling it multiple times will read the same data
     * unless the data in the topic changes.
     *
     * @param topic topic to read from
     * @param timeout consumer poll timeout
     * @return consumed records
     */
    public List<ConsumerRecord<K, V>> from(final String topic, final Duration timeout) {
        try (final Consumer<K, V> consumer = this.createConsumer()) {
            return readAll(consumer, topic, timeout);
        }
    }

    /**
     * Create a new {@code Consumer} for a Kafka cluster
     *
     * @return {@code Consumer}
     */
    public Consumer<K, V> createConsumer() {
        return new KafkaConsumer<>(this.properties,
                this.keyDeserializer.configureForKeys(this.properties),
                this.valueDeserializer.configureForValues(this.properties));
    }

    /**
     * Provide custom deserializers for keys and values. Deserializers are configured automatically.
     *
     * @param keyDeserializer serializer for keys
     * @param valueDeserializer serializer for values
     * @param <KN> type of keys
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom deserializers
     */
    public <KN, VN> ReaderBuilder<KN, VN> withDeserializers(final Preconfigured<Deserializer<KN>> keyDeserializer,
            final Preconfigured<Deserializer<VN>> valueDeserializer) {
        return new ReaderBuilder<>(this.properties, keyDeserializer, valueDeserializer);
    }

    /**
     * Provide custom deserializers for keys and values. Deserializers are configured automatically.
     * @param keyDeserializer serializer for keys
     * @param valueDeserializer serializer for values
     * @param <KN> type of keys
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom deserializers
     * @see ReaderBuilder#withDeserializers(Preconfigured, Preconfigured)
     */
    public <KN, VN> ReaderBuilder<KN, VN> withDeserializers(final Deserializer<KN> keyDeserializer,
            final Deserializer<VN> valueDeserializer) {
        return this.withDeserializers(Preconfigured.create(keyDeserializer), Preconfigured.create(valueDeserializer));
    }

    /**
     * Provide custom deserializers for keys. Deserializer is configured automatically.
     * @param keyDeserializer serializer for keys
     * @param <KN> type of keys
     * @return {@code SenderBuilder} with custom key deserializer
     */
    public <KN> ReaderBuilder<KN, V> withKeyDeserializer(final Preconfigured<Deserializer<KN>> keyDeserializer) {
        return this.withDeserializers(keyDeserializer, this.valueDeserializer);
    }

    /**
     * Provide custom deserializers for keys. Deserializer is configured automatically.
     * @param keyDeserializer serializer for keys
     * @param <KN> type of keys
     * @return {@code SenderBuilder} with custom key deserializer
     * @see ReaderBuilder#withKeyDeserializer(Preconfigured)
     */
    public <KN> ReaderBuilder<KN, V> withKeyDeserializer(final Deserializer<KN> keyDeserializer) {
        return this.withKeyDeserializer(Preconfigured.create(keyDeserializer));
    }

    /**
     * Provide custom deserializers for values. Deserializer is configured automatically.
     * @param valueDeserializer serializer for values
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom values deserializer
     */
    public <VN> ReaderBuilder<K, VN> withValueDeserializer(final Preconfigured<Deserializer<VN>> valueDeserializer) {
        return this.withDeserializers(this.keyDeserializer, valueDeserializer);
    }

    /**
     * Provide custom deserializers for values. Deserializer is configured automatically.
     * @param valueDeserializer serializer for values
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom values deserializer
     * @see ReaderBuilder#withValueDeserializer(Preconfigured)
     */
    public <VN> ReaderBuilder<K, VN> withValueDeserializer(final Deserializer<VN> valueDeserializer) {
        return this.withValueDeserializer(Preconfigured.create(valueDeserializer));
    }

}
