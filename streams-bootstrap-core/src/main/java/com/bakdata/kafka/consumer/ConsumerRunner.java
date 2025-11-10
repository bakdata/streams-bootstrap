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

import com.bakdata.kafka.Runner;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * Runs a Kafka Consumer application
 */
@RequiredArgsConstructor
@Slf4j
public class ConsumerRunner implements Runner {

    private final @NonNull ConsumerRunnable runnable;
    private final @NonNull ConsumerConfig config;
    private final @NonNull ConsumerExecutionOptions executionOptions;

    @Override
    public void close() {
        log.info("Closing consumer");
        this.runnable.close();
    }

    @Override
    public void run() {
        log.info("Starting consumer");
        this.runConsumer();
    }

    private void runConsumer() {
        log.info("Starting Kafka Consumer and calling start hook");
        final RunningConsumer runningStreams = RunningConsumer.builder()
                .consumerRunnable(this.runnable)
                .config(this.config)
                .build();
        this.executionOptions.onStart(runningStreams);
        // Run Kafka consumer until it shuts down
        this.runnable.run(this.config);
    }
}
