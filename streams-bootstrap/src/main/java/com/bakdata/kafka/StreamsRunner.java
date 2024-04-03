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

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

@Slf4j
public final class StreamsRunner implements AutoCloseable {

    private final @NonNull KafkaStreams streams;
    private final @NonNull CapturingStreamsUncaughtExceptionHandler exceptionHandler;
    private final @NonNull StreamsShutdownStateListener shutdownListener;
    private final @NonNull CloseOptions closeOptions;
    private final @NonNull StreamsHooks hooks;

    @Builder
    private StreamsRunner(final @NonNull Topology topology, final @NonNull StreamsConfig config,
            final @NonNull StreamsExecutionOptions executionOptions, final @NonNull StreamsHooks hooks) {
        this.streams = new KafkaStreams(topology, config);
        this.exceptionHandler = new CapturingStreamsUncaughtExceptionHandler(hooks.getUncaughtExceptionHandler());
        this.streams.setUncaughtExceptionHandler(this.exceptionHandler);
        this.shutdownListener = new StreamsShutdownStateListener(hooks.getStateListener());
        this.streams.setStateListener(this.shutdownListener);
        this.closeOptions = executionOptions.createCloseOptions(config);
        this.hooks = hooks;
    }

    private static boolean isError(final State newState) {
        return newState == State.ERROR;
    }

    /**
     * Run the Streams application. This method blocks until Kafka Streams has completed shutdown, either because it
     * caught an error or the application has received a shutdown event.
     */
    public void run() {
        this.runStreams();
        this.awaitStreamsShutdown();
        this.checkErrors();
    }

    @Override
    public void close() {
        log.info("Closing Kafka Streams");
        final boolean success = this.streams.close(this.closeOptions);
        if (success) {
            log.info("Successfully closed Kafka Streams");
        } else {
            log.info("Timed out closing Kafka Streams");
        }
    }

    private void checkErrors() {
        if (isError(this.streams.state())) {
            this.exceptionHandler.throwException();
        }
    }

    /**
     * Start Kafka Streams and register a ShutdownHook for closing Kafka Streams.
     */
    private void runStreams() {
        log.info("Starting Kafka Streams");
        this.streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        log.info("Calling start hook");
        this.hooks.onStart(this.streams);
    }

    /**
     * Wait for Kafka Streams to shut down. Shutdown is detected by a {@link StateListener}.
     *
     * @see State#hasCompletedShutdown()
     */
    private void awaitStreamsShutdown() {
        try {
            this.shutdownListener.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamsApplicationException("Error awaiting Streams shutdown", e);
        }
    }

}
