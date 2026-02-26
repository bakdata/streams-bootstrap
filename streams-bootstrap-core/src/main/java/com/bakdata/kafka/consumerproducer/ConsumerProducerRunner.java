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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.Runner;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Runs a Kafka Consumer and Producer application
 */
@RequiredArgsConstructor
@Slf4j
public class ConsumerProducerRunner implements Runner {

    private final @NonNull ConsumerProducerRunnable runnable;
    private final @NonNull ConsumerConfig config;
    private final @NonNull ConsumerProducerExecutionOptions executionOptions;
    private final @NonNull ConcurrentLinkedDeque<Consumer<?, ?>> consumers;
    private final @NonNull ConcurrentLinkedDeque<Producer<?, ?>> producers;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public void close() {
        log.info("Closing consumer and producer");
        if (!this.running.compareAndSet(true, false)) {
            log.info("ConsumerProducer is not running or already stopping");
            return;
        }
        this.consumers.forEach(Consumer::wakeup);
        try {
            this.shutdownLatch.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsumerProducerApplicationException("Error awaiting ConsumerProducer shutdown", e);
        }
        this.producers.forEach(Producer::close);
        this.runnable.close();
    }

    @Override
    public void run() {
        log.info("Starting Kafka ConsumerProducer");
        if (!this.running.compareAndSet(false, true)) {
            throw new ConsumerProducerApplicationException("ConsumerProducer already running");
        }
        // Run Kafka application until it shuts down
        try {
            while (this.running.get()) {
                this.runnable.run(this.executionOptions.getPollTimeout());
            }
        } catch (final WakeupException e) {
            log.info("Got woken up", e);
        } finally {
            final CloseOptions closeOptions =
                    this.executionOptions.toConsumerExecutionOptions().createCloseOptions(this.config);
            this.consumers.forEach(consumer -> consumer.close(closeOptions));
            this.shutdownLatch.countDown();
        }
    }
}
