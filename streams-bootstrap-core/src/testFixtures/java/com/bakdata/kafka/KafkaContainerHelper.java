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
import com.bakdata.kafka.util.TopicSettings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.testcontainers.kafka.KafkaContainer;

@RequiredArgsConstructor
public class KafkaContainerHelper {

    public static final TopicSettings DEFAULT_TOPIC_SETTINGS = TopicSettings.builder()
            .partitions(1)
            .replicationFactor((short) 1)
            .build();
    private final @NonNull KafkaContainer kafkaCluster;

    private static <K, V> List<ConsumerRecord<K, V>> pollAll(final Consumer<K, V> consumer, final Duration timeout) {
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        ConsumerRecords<K, V> poll = consumer.poll(timeout);
        while (!poll.isEmpty()) {
            poll.forEach(records::add);
            poll = consumer.poll(timeout);
        }
        return records;
    }

    public SenderBuilder send() {
        return new SenderBuilder(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));
    }

    public ReaderBuilder read() {
        return new ReaderBuilder(Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
    }

    public ImprovedAdminClient admin() {
        return ImprovedAdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContainerHelper.this.kafkaCluster.getBootstrapServers()
        ));
    }

    @RequiredArgsConstructor
    public class SenderBuilder {

        private final @NonNull Map<String, Object> properties;

        public SenderBuilder with(final String key, final Object value) {
            final Map<String, Object> newProperties = new HashMap<>(this.properties);
            newProperties.put(key, value);
            return new SenderBuilder(Map.copyOf(newProperties));
        }

        public <K, V> void to(final String topic, final Iterable<? extends KeyValue<K, V>> records) {
            final Map<String, Object> producerConfig = new HashMap<>(this.properties);
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    KafkaContainerHelper.this.kafkaCluster.getBootstrapServers());
            try (final Producer<K, V> producer = new KafkaProducer<>(producerConfig)) {
                records.forEach(kv -> producer.send(new ProducerRecord<>(topic, kv.key, kv.value)));
            }
        }

    }

    @RequiredArgsConstructor
    public class ReaderBuilder {

        private final @NonNull Map<String, Object> properties;

        public ReaderBuilder with(final String key, final Object value) {
            final Map<String, Object> newProperties = new HashMap<>(this.properties);
            newProperties.put(key, value);
            return new ReaderBuilder(Map.copyOf(newProperties));
        }

        public <K, V> List<ConsumerRecord<K, V>> from(final String output, final Duration timeout) {
            final Map<String, Object> consumerConfig = new HashMap<>(this.properties);
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    KafkaContainerHelper.this.kafkaCluster.getBootstrapServers());
            try (final Consumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
                final List<PartitionInfo> partitionInfos = consumer.listTopics().get(output);
                final List<TopicPartition> topicPartitions = partitionInfos.stream()
                        .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                        .collect(Collectors.toList());
                consumer.assign(topicPartitions);
                consumer.seekToBeginning(topicPartitions);
                return pollAll(consumer, timeout);
            }
        }

    }
}
