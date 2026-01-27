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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.Runner;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Runs a Kafka Consumer and Producer application
 */
@RequiredArgsConstructor
@Slf4j
public class ConsumerProducerRunner implements Runner {

    private final @NonNull ConsumerProducerRunnable runnable;
    private final @NonNull ConsumerConfig consumerConfig;
    private final @NonNull ProducerConfig producerConfig;
    private final @NonNull ConsumerProducerExecutionOptions executionOptions;

    @Override
    public void close() {
        log.info("Closing consumer and producer");
        this.runnable.close();
    }

    @Override
    public void run() {
        log.info("Starting consumer and producer");
        this.runConsumerProducer();
    }

    private void runConsumerProducer() {
        log.info("Starting Kafka ConsumerProducer and calling start hook");
        final RunningConsumerProducer runningConsumer = RunningConsumerProducer.builder()
                .consumerProducerRunnable(this.runnable)
                .consumerConfig(this.consumerConfig)
                .producerConfig(this.producerConfig)
                .build();
        this.executionOptions.onStart(runningConsumer);
        // Run Kafka application until it shuts down
        this.runnable.run(this.consumerConfig, this.producerConfig);
    }
}
