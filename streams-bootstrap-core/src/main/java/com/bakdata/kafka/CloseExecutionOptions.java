/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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
import java.util.Map;
import lombok.Builder;
import org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Options to configure closing behavior of Kafka apps
 */
@Builder
public class CloseExecutionOptions {
    /**
     * Defines if {@link ConsumerConfig#GROUP_INSTANCE_ID_CONFIG} is volatile. If it is configured and non-volatile,
     * {@link CloseOptions#leaveGroup(boolean)} is disabled and
     * {@link org.apache.kafka.clients.consumer.CloseOptions#withGroupMembershipOperation(GroupMembershipOperation)} is
     * set to {@link GroupMembershipOperation#DEFAULT}.
     */
    @Builder.Default
    private final boolean volatileGroupInstanceId = true;
    /**
     * Defines {@link CloseOptions#timeout(Duration)} and
     * {@link org.apache.kafka.clients.consumer.CloseOptions#withTimeout(Duration)}
     */
    @Builder.Default
    private final Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    private static boolean isStaticMembershipDisabled(final Map<String, Object> originals) {
        return originals.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) == null;
    }

    /**
     * Create {@link CloseOptions} for {@link org.apache.kafka.streams.KafkaStreams}
     *
     * @param config streams config
     * @return {@link CloseOptions}
     * @see org.apache.kafka.streams.KafkaStreams#close(CloseOptions)
     */
    public CloseOptions createCloseOptions(final StreamsConfig config) {
        final boolean leaveGroup = this.shouldLeaveGroup(config.originals());
        return new CloseOptions().leaveGroup(leaveGroup).timeout(this.closeTimeout);
    }

    /**
     * Create {@link org.apache.kafka.clients.consumer.CloseOptions} for
     * {@link org.apache.kafka.clients.consumer.Consumer}
     *
     * @param config consumer config
     * @return {@link org.apache.kafka.clients.consumer.CloseOptions}
     * @see org.apache.kafka.clients.consumer.Consumer#close(org.apache.kafka.clients.consumer.CloseOptions)
     */
    public org.apache.kafka.clients.consumer.CloseOptions createCloseOptions(final ConsumerConfig config) {
        final boolean leaveGroup = this.shouldLeaveGroup(config.originals());
        final GroupMembershipOperation operation =
                leaveGroup ? GroupMembershipOperation.LEAVE_GROUP : GroupMembershipOperation.DEFAULT;
        return org.apache.kafka.clients.consumer.CloseOptions.groupMembershipOperation(operation)
                .withTimeout(this.closeTimeout);
    }

    boolean shouldLeaveGroup(final Map<String, Object> originals) {
        final boolean staticMembershipDisabled = isStaticMembershipDisabled(originals);
        return staticMembershipDisabled || this.volatileGroupInstanceId;
    }
}
