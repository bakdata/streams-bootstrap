/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Options to run a Kafka Streams app using {@link StreamsRunner}
 */
@Builder
public class StreamsExecutionOptions {
    /**
     * Hook that is called after calling {@link KafkaStreams#start()}
     */
    @Builder.Default
    private final @NonNull Consumer<KafkaStreams> onStart = streams -> {};
    /**
     * Configures {@link KafkaStreams#setStateListener(StateListener)}
     */
    @Builder.Default
    private final @NonNull Supplier<StateListener> stateListener = NoOpStateListener::new;
    /**
     * Configures {@link KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)}
     */
    @Builder.Default
    private final @NonNull Supplier<StreamsUncaughtExceptionHandler> uncaughtExceptionHandler =
            DefaultUncaughtExceptionHandler::new;
    /**
     * Defines if {@link ConsumerConfig#GROUP_INSTANCE_ID_CONFIG} is volatile. If it is configured and non-volatile,
     * {@link KafkaStreams#close(CloseOptions)} is called with {@link CloseOptions#leaveGroup(boolean)} disabled
     */
    @Builder.Default
    private final boolean volatileGroupInstanceId = true;
    /**
     * Defines {@link CloseOptions#timeout(Duration)} when calling {@link KafkaStreams#close(CloseOptions)}
     */
    @Builder.Default
    private final Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    private static boolean isStaticMembershipDisabled(final StreamsConfig config) {
        return config.originals().get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG) == null;
    }

    CloseOptions createCloseOptions(final StreamsConfig config) {
        final boolean leaveGroup = this.shouldLeaveGroup(config);
        return new CloseOptions().leaveGroup(leaveGroup).timeout(this.closeTimeout);
    }

    @VisibleForTesting
    boolean shouldLeaveGroup(final StreamsConfig config) {
        final boolean staticMembershipDisabled = isStaticMembershipDisabled(config);
        return staticMembershipDisabled || this.volatileGroupInstanceId;
    }

    void onStart(final KafkaStreams streams) {
        this.onStart.accept(streams);
    }

    StreamsUncaughtExceptionHandler createUncaughtExceptionHandler() {
        return this.uncaughtExceptionHandler.get();
    }

    StateListener createStateListener() {
        return this.stateListener.get();
    }
}
