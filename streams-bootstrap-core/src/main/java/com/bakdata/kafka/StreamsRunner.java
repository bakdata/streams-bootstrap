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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.CloseOptions;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Runs a Kafka Streams application
 */
@Slf4j
public final class StreamsRunner implements Runner {

    private final @NonNull StreamsConfig config;
    private final @NonNull KafkaStreams streams;
    private final @NonNull CapturingStreamsUncaughtExceptionHandler exceptionHandler;
    private final @NonNull StreamsShutdownStateListener shutdownListener;
    private final @NonNull CloseOptions closeOptions;
    private final @NonNull StreamsExecutionOptions executionOptions;

    /**
     * Create a {@code StreamsRunner} with default {@link StreamsExecutionOptions}
     * @param topology topology to be executed
     * @param config streams configuration
     */
    public StreamsRunner(final @NonNull Topology topology, final @NonNull StreamsConfig config) {
        this(topology, config, StreamsExecutionOptions.builder().build());
    }

    /**
     * Create a {@code StreamsRunner}
     * @param topology topology to be executed
     * @param config streams configuration
     * @param options options to customize {@link KafkaStreams} behavior
     */
    public StreamsRunner(final @NonNull Topology topology, final @NonNull StreamsConfig config,
            final @NonNull StreamsExecutionOptions options) {
        this.config = config;
        this.streams = new KafkaStreams(topology, config);
        this.exceptionHandler = new CapturingStreamsUncaughtExceptionHandler(options.createUncaughtExceptionHandler());
        this.streams.setUncaughtExceptionHandler(this.exceptionHandler);
        this.shutdownListener = new StreamsShutdownStateListener(options.createStateListener());
        this.streams.setStateListener(this.shutdownListener);
        this.closeOptions = options.createCloseOptions(config);
        this.executionOptions = options;
    }

    /**
     * Run the Streams application. This method blocks until Kafka Streams has completed shutdown, either because it
     * caught an error or {@link #close()} has been called.
     */
    @Override
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
        if (this.hasErrored()) {
            this.exceptionHandler.throwException();
        }
    }

    private boolean hasErrored() {
        return this.streams.state() == State.ERROR;
    }

    private void runStreams() {
        log.info("Starting Kafka Streams");
        this.streams.start();
        log.info("Calling start hook");
        this.executionOptions.onStart(this.streams, this.config);
    }

    private void awaitStreamsShutdown() {
        try {
            this.shutdownListener.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamsApplicationException("Error awaiting Streams shutdown", e);
        }
    }

}
