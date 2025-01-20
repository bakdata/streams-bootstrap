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
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;

/**
 * Utility class to verify the state of a Kafka consumer group
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupVerifier {

    private final @NonNull String group;
    private final @NonNull Supplier<ImprovedAdminClient> adminClientSupplier;

    /**
     * Create a new verifier from an {@code ExecutableStreamsApp}
     * @param app app to create verifier from
     * @return verifier
     */
    public static ConsumerGroupVerifier verify(final ExecutableStreamsApp<?> app) {
        final EffectiveAppConfiguration<StreamsTopicConfig> config = app.getEffectiveConfig();
        final ImprovedStreamsConfig streamsConfig = new ImprovedStreamsConfig(app.getConfig());
        return new ConsumerGroupVerifier(streamsConfig.getAppId(), config::createAdminClient);
    }

    /**
     * Check whether consumer group has state {@link ConsumerGroupState#STABLE}
     * @return true if consumer group has state {@link ConsumerGroupState#STABLE}
     */
    public boolean isActive() {
        return this.getState() == ConsumerGroupState.STABLE;
    }

    /**
     * Check whether consumer group has state {@link ConsumerGroupState#EMPTY}
     * @return true if consumer group has state {@link ConsumerGroupState#EMPTY}
     */
    public boolean isClosed() {
        return this.getState() == ConsumerGroupState.EMPTY;
    }

    /**
     * Get current state of consumer group
     * @return current state of consumer group
     */
    public ConsumerGroupState getState() {
        try (final ImprovedAdminClient admin = this.adminClientSupplier.get();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient()) {
            final ConsumerGroupDescription description = consumerGroupClient.describe(this.group);
            final ConsumerGroupState state = description.state();
            log.debug("Consumer group '{}' has state {}", this.group, state);
            return state;
        }
    }

    /**
     * Check whether consumer group has assigned partitions and lag is 0
     * @return true if consumer group has assigned partitions and lag is 0
     */
    public boolean hasFinishedProcessing() {
        return this.computeLag().filter(lag -> lag == 0).isPresent();
    }

    /**
     * Compute lag of consumer group
     * @return lag of consumer group. If no partitions are assigned, an empty {@code Optional} is returned
     */
    public Optional<Long> computeLag() {
        try (final ImprovedAdminClient admin = this.adminClientSupplier.get();
                final ConsumerGroupClient consumerGroupClient = admin.getConsumerGroupClient();
                final TopicClient topicClient = admin.getTopicClient()) {
            final Map<TopicPartition, OffsetAndMetadata> consumerOffsets =
                    consumerGroupClient.listOffsets(this.group);
            log.debug("Consumer group '{}' has {} subscribed partitions", this.group, consumerOffsets.size());
            if (consumerOffsets.isEmpty()) {
                return Optional.empty();
            }
            final Map<TopicPartition, ListOffsetsResultInfo> partitionOffsets =
                    topicClient.listOffsets(consumerOffsets.keySet());
            final long lag = consumerOffsets.entrySet().stream()
                    .mapToLong(e -> {
                        final TopicPartition topicPartition = e.getKey();
                        final OffsetAndMetadata consumerOffset = e.getValue();
                        final ListOffsetsResultInfo partitionOffset = partitionOffsets.get(topicPartition);
                        return partitionOffset.offset() - consumerOffset.offset();
                    })
                    .sum();
            log.debug("Consumer group '{}' has lag {}", this.group, lag);
            return Optional.of(lag);
        }
    }
}
