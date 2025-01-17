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
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@RequiredArgsConstructor
public class ProgressVerifier {

    private final KafkaTestClient testClient;

    public boolean isActive(final String group) {
        return this.getState(group) == ConsumerGroupState.STABLE;
    }

    public boolean isClosed(final String group) {
        return this.getState(group) == ConsumerGroupState.EMPTY;
    }

    public boolean hasFinishedProcessing(final String group) {
        return this.computeLag(group) == 0;
    }

    public boolean hasMessages(final String topic, final int numberOfMessages, final Duration timeout) {
        final List<ConsumerRecord<byte[], byte[]>> records = this.testClient.read()
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .from(topic, timeout);
        return records.size() == numberOfMessages;
    }

    private long computeLag(final String group) {
        try (final ImprovedAdminClient admin = this.testClient.admin();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient();
                final TopicClient topicClient = admin.getTopicClient()) {
            final Map<TopicPartition, OffsetAndMetadata> consumerOffsetsMap = consumerGroupClient.listOffsets(group);
            final PairSeq<TopicPartition, Long> consumerOffsets = PairSeq.seq(consumerOffsetsMap)
                    .mapValues(OffsetAndMetadata::offset);
            final Map<TopicPartition, ListOffsetsResultInfo> partitionOffsetsMap =
                    topicClient.listOffsets(consumerOffsetsMap.keySet());
            final PairSeq<TopicPartition, Long> partitionOffsets = PairSeq.seq(partitionOffsetsMap)
                    .mapValues(ListOffsetsResultInfo::offset);
            return consumerOffsets.innerJoinByKey(partitionOffsets)
                    .values()
                    .mapToPair(Function.identity())
                    .map((consumerOffset, latestOffset) -> latestOffset - consumerOffset)
                    .sum()
                    .orElse(0L);
        }
    }

    private ConsumerGroupState getState(final String group) {
        try (final ImprovedAdminClient admin = this.testClient.admin();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient()) {
            final ConsumerGroupDescription description = consumerGroupClient.describe(group);
            return description.state();
        }
    }
}
