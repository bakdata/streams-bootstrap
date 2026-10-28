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

import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient;
import com.bakdata.kafka.admin.StreamsGroupsClient;
import com.bakdata.kafka.admin.TopicsClient;
import com.bakdata.kafka.consumer.ExecutableConsumerApp;
import com.bakdata.kafka.consumerproducer.ConfiguredConsumerProducerApp;
import com.bakdata.kafka.consumerproducer.ExecutableConsumerProducerApp;
import com.bakdata.kafka.streams.ExecutableStreamsApp;
import com.bakdata.kafka.streams.StreamsConfigX;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;

/**
 * Utility class to verify the state of a Kafka consumer group or streams group
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupVerifier {

    private final @NonNull String group;
    private final @NonNull Supplier<AdminClientX> adminClientSupplier;
    private final boolean streamsGroupProtocol;

    /**
     * Create a new verifier for a classic consumer group.
     */
    public ConsumerGroupVerifier(final @NonNull String group,
            final @NonNull Supplier<AdminClientX> adminClientSupplier) {
        this(group, adminClientSupplier, false);
    }

    /**
     * Create a new verifier from an {@link ExecutableStreamsApp}
     * @param app app to create verifier from
     * @return verifier
     */
    public static ConsumerGroupVerifier verify(final ExecutableStreamsApp<?> app) {
        final Map<String, Object> kafkaProperties = app.getKafkaProperties();
        final StreamsConfigX streamsConfig = new StreamsConfigX(app.getConfig());
        return new ConsumerGroupVerifier(streamsConfig.getAppId(), () -> AdminClientX.create(kafkaProperties),
                streamsConfig.isStreamsGroupProtocol());
    }

    /**
     * Create a new verifier from an {@code ExecutableConsumerApp}
     * @param app app to create verifier from
     * @return verifier
     */
    public static ConsumerGroupVerifier verify(final ExecutableConsumerApp<?> app) {
        final Map<String, Object> kafkaProperties = app.getKafkaProperties();
        return new ConsumerGroupVerifier(app.getGroupId(), () -> AdminClientX.create(kafkaProperties));
    }

    /**
     * Create a new verifier from an {@code ExecutableConsumerProducerApp}
     * @param app app to create verifier from
     * @return verifier
     */
    public static ConsumerGroupVerifier verify(final ExecutableConsumerProducerApp<?> app) {
        final Map<String, Object> kafkaProperties = app.getConsumerProperties();
        return new ConsumerGroupVerifier(app.getGroupId(), () -> AdminClientX.create(kafkaProperties));
    }

    /**
     * Check whether consumer group has state {@link GroupState#STABLE}
     * @return true if consumer group has state {@link GroupState#STABLE}
     */
    public boolean isActive() {
        return this.getState().filter(s -> s == GroupState.STABLE).isPresent();
    }

    /**
     * Check whether consumer group has state {@link GroupState#EMPTY}
     * @return true if consumer group has state {@link GroupState#EMPTY}
     */
    public boolean isClosed() {
        return this.getState().filter(s -> s == GroupState.EMPTY).isPresent();
    }

    /**
     * Get current state of consumer group or streams group
     *
     * @return current state of the group
     */
    public Optional<GroupState> getState() {
        try (final AdminClientX admin = this.adminClientSupplier.get()) {
            if (this.streamsGroupProtocol) {
                return this.getStreamsGroupState(admin);
            }
            return this.getConsumerGroupState(admin);
        }
    }

    private Optional<GroupState> getConsumerGroupState(final AdminClientX admin) {
        final ConsumerGroupsClient groups = admin.consumerGroups();
        return groups.group(this.group).describe()
                .map(this::getState);
    }

    private GroupState getState(final ConsumerGroupDescription description) {
        final GroupState state = description.groupState();
        log.debug("Consumer group '{}' has state {}", this.group, state);
        return state;
    }

    private Optional<GroupState> getStreamsGroupState(final AdminClientX admin) {
        final StreamsGroupsClient streamsGroups = admin.streamsGroups();
        return streamsGroups.group(this.group).describe()
                .map(this::getState);
    }

    private GroupState getState(final StreamsGroupDescription description) {
        final GroupState state = description.groupState();
        log.debug("Streams group '{}' has state {}", this.group, state);
        return state;
    }

    /**
     * Check whether consumer group has assigned partitions and lag is 0
     * @return true if consumer group has assigned partitions and lag is 0
     */
    public boolean hasFinishedProcessing() {
        return this.computeLag().filter(lag -> lag == 0).isPresent();
    }

    /**
     * Compute lag of consumer group or streams group
     * @return lag of the group. If no partitions are assigned, an empty {@link Optional} is returned
     */
    public Optional<Long> computeLag() {
        try (final AdminClientX admin = this.adminClientSupplier.get()) {
            final Map<TopicPartition, OffsetAndMetadata> groupOffsets;
            if (this.streamsGroupProtocol) {
                final StreamsGroupsClient streamsGroups = admin.streamsGroups();
                groupOffsets = streamsGroups.group(this.group).listOffsets();
            } else {
                final ConsumerGroupsClient groups = admin.consumerGroups();
                groupOffsets = groups.group(this.group).listOffsets();
            }
            log.debug("Group '{}' has {} subscribed partitions", this.group, groupOffsets.size());
            if (groupOffsets.isEmpty()) {
                return Optional.empty();
            }
            final TopicsClient topics = admin.topics();
            final Map<TopicPartition, ListOffsetsResultInfo> partitionOffsets =
                    topics.listOffsets(groupOffsets.keySet());
            final long lag = groupOffsets.entrySet().stream()
                    .mapToLong(e -> {
                        final TopicPartition topicPartition = e.getKey();
                        final OffsetAndMetadata consumerOffset = e.getValue();
                        final ListOffsetsResultInfo partitionOffset = partitionOffsets.get(topicPartition);
                        return partitionOffset.offset() - consumerOffset.offset();
                    })
                    .sum();
            log.debug("Group '{}' has lag {}", this.group, lag);
            return Optional.of(lag);
        }
    }
}
