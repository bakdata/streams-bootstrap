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

import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import com.bakdata.util.seq2.PairSeq;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Utility class to verify the state of Kafka consumer groups and topics
 */
@Slf4j
@RequiredArgsConstructor
public class ProgressVerifier {

    private final KafkaTestClient testClient;

    /**
     * Check whether consumer group has state {@link ConsumerGroupState#STABLE}
     * @param group consumer group name
     * @return true if consumer group has state {@link ConsumerGroupState#STABLE}
     */
    public boolean isActive(final String group) {
        return this.getState(group) == ConsumerGroupState.STABLE;
    }

    /**
     * Check whether consumer group has state {@link ConsumerGroupState#EMPTY}
     * @param group consumer group name
     * @return true if consumer group has state {@link ConsumerGroupState#EMPTY}
     */
    public boolean isClosed(final String group) {
        return this.getState(group) == ConsumerGroupState.EMPTY;
    }

    /**
     * Get current state of consumer group
     * @param group consumer group name
     * @return current state of consumer group
     */
    public ConsumerGroupState getState(final String group) {
        try (final ImprovedAdminClient admin = this.testClient.admin();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient()) {
            final ConsumerGroupDescription description = consumerGroupClient.describe(group);
            final ConsumerGroupState state = description.state();
            log.debug("Consumer group '{}' has state {}", group, state);
            return state;
        }
    }

    /**
     * Check whether consumer group has assigned partitions and lag is 0
     * @param group consumer group name
     * @return true if consumer group has assigned partitions and lag is 0
     */
    public boolean hasFinishedProcessing(final String group) {
        return this.computeLag(group).filter(lag -> lag == 0).isPresent();
    }

    /**
     * Compute lag of consumer group
     * @param group consumer group name
     * @return lag of consumer group. If no partitions are assigned, an empty {@code Optional} is returned
     */
    public Optional<Long> computeLag(final String group) {
        try (final ImprovedAdminClient admin = this.testClient.admin();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient();
                final TopicClient topicClient = admin.getTopicClient()) {
            final Map<TopicPartition, OffsetAndMetadata> consumerOffsetsMap = consumerGroupClient.listOffsets(group);
            log.debug("Consumer group '{}' has {} subscribed partitions", group, consumerOffsetsMap.size());
            final PairSeq<TopicPartition, Long> consumerOffsets = PairSeq.seq(consumerOffsetsMap)
                    .mapValues(OffsetAndMetadata::offset);
            final Map<TopicPartition, ListOffsetsResultInfo> partitionOffsetsMap =
                    topicClient.listOffsets(consumerOffsetsMap.keySet());
            final PairSeq<TopicPartition, Long> partitionOffsets = PairSeq.seq(partitionOffsetsMap)
                    .mapValues(ListOffsetsResultInfo::offset);
            final Optional<Long> lag = consumerOffsets.innerJoinByKey(partitionOffsets)
                    .values()
                    .mapToPair(Function.identity())
                    .map((consumerOffset, latestOffset) -> latestOffset - consumerOffset)
                    .sum();
            log.debug("Consumer group '{}' has lag {}", group, lag);
            return lag;
        }
    }

    /**
     * Read all messages from a topic as bytes
     * @param topic topic name
     * @param timeout consumer poll timeout
     * @return all messages from topic
     */
    public List<ConsumerRecord<byte[], byte[]>> readAllMessages(final String topic, final Duration timeout) {
        final List<ConsumerRecord<byte[], byte[]>> records = this.testClient.read()
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .from(topic, timeout);
        log.debug("Topic '{}' has {} messages", records, records.size());
        return records;
    }
}
