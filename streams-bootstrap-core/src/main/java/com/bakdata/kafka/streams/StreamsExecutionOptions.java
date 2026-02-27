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

package com.bakdata.kafka.streams;

import com.bakdata.kafka.CloseExecutionOptions;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.NonNull;
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
    private final @NonNull Consumer<RunningStreams> onStart = runningStreams -> {};
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
            DefaultStreamsUncaughtExceptionHandler::new;
    /**
     * Defines the behavior when closing a Kafka Streams app using {@link KafkaStreams#close(CloseOptions)}.
     */
    @Builder.Default
    private final CloseExecutionOptions closeExecutionOptions = CloseExecutionOptions.builder()
            .build();
    /**
     * Defines {@link CloseOptions#timeout(Duration)} when calling {@link KafkaStreams#close(CloseOptions)}
     */
    @Builder.Default
    private final Duration closeTimeout = Duration.ofMillis(Long.MAX_VALUE);

    CloseOptions createCloseOptions(final StreamsConfig config) {
        return this.closeExecutionOptions.createCloseOptions(config);
    }

    void onStart(final RunningStreams runningStreams) {
        this.onStart.accept(runningStreams);
    }

    StreamsUncaughtExceptionHandler createUncaughtExceptionHandler() {
        return this.uncaughtExceptionHandler.get();
    }

    StateListener createStateListener() {
        return this.stateListener.get();
    }
}
