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

package com.bakdata.kafka.consumer;

import java.time.Duration;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Options to run a Kafka Consumer app
 */
@Builder
public final class ConsumerExecutionOptions {

    /**
     * Hook that is called after the {@link ConsumerRunnable} is started
     */
    @Builder.Default
    private final @NonNull java.util.function.Consumer<RunningConsumer> onStart = runningStreams -> {};

    /**
     * Defines if {@link ConsumerConfig#GROUP_INSTANCE_ID_CONFIG} is volatile. If it is configured and non-volatile,
     * {@link Consumer#close(CloseOptions)} is called with
     * {@link CloseOptions#groupMembershipOperation(GroupMembershipOperation)} set to
     * {@link GroupMembershipOperation#REMAIN_IN_GROUP}
     */
    @Builder.Default
    private final boolean volatileGroupInstanceId = true;

    /**
     * Defines {@link CloseOptions#timeout(Duration)} when calling {@link Consumer#close(CloseOptions)}
     */
    @Builder.Default
    private final Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    private static boolean isStaticMembershipDisabled(final Map<String, Object> originals) {
        return originals.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) == null;
    }

    CloseOptions createCloseOptions(final ConsumerConfig config) {
        final boolean leaveGroup = this.shouldLeaveGroup(config.originals());
        final GroupMembershipOperation operation =
                leaveGroup ? GroupMembershipOperation.LEAVE_GROUP : GroupMembershipOperation.DEFAULT;
        return CloseOptions.groupMembershipOperation(operation).withTimeout(this.closeTimeout);
    }

    // TODO consumerexecutionoptionstest and other tests
    boolean shouldLeaveGroup(final Map<String, Object> originals) {
        final boolean staticMembershipDisabled = isStaticMembershipDisabled(originals);
        return staticMembershipDisabled || this.volatileGroupInstanceId;
    }

    void onStart(final RunningConsumer runningConsumer) {
        this.onStart.accept(runningConsumer);
    }
}
