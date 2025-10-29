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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

@RequiredArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
public class DefaultConsumerRunnable<K, V> implements ConsumerRunnable {

    // TODO extensively test this runnable
    // TODO add Javadocs

    @Getter
    private final Consumer<K, V> consumer;
    private final ConsumerTopicConfig topics;
    private final ConsumerExecutionOptions executionOptions;
    private final RecordProcessor<K, V> recordProcessor;
    private ConsumerConfig consumerConfig = null;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean(false);
    // TODO start/shutdown hooks?

    // TODO make configurable?
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    // TODO make configurable?
    private static final long SHUTDOWN_TIMEOUT = 1000;

    @Override
    public void run(final ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
        if(!this.running.compareAndSet(false, true)) {
            log.warn("Consumer already running");
            return;
        }
        this.withAllTopicsSubscribed();
        this.pollLoop();
    }

    public void withAllTopicsSubscribed() {
        // TODO does it make sense to subscribe to all topics? - TODO maybe ConsumerBuilder.withAllTopicsSubscribed? - fully support fluent api
        if(!this.topics.getInputTopics().isEmpty()) {
            this.consumer.subscribe(this.topics.getInputTopics());
        }
        if(!this.topics.getLabeledInputTopics().isEmpty()) {
            this.topics.getLabeledInputTopics().values().forEach(this.consumer::subscribe);
        }
        if(this.topics.getInputPattern() != null) {
            this.consumer.subscribe(this.topics.getInputPattern());
        }
        if(!this.topics.getLabeledInputPatterns().isEmpty()) {
            this.topics.getLabeledInputPatterns().values().forEach(this.consumer::subscribe);
        }
    }

    private void pollLoop() {
        try {
            while(this.running.get()) {
                final ConsumerRecords<K, V> consumerRecords = this.consumer.poll(POLL_TIMEOUT);
                if(!consumerRecords.isEmpty()) {
                    log.debug("Polled {} records", consumerRecords.count());
                    this.recordProcessor.processRecords(consumerRecords);
                    this.consumer.commitSync();
                }
            }
        } catch (final WakeupException exception) {
            log.info("Consumer poll loop waking up for shutdown");
        } catch (final RuntimeException exception) {
            log.error("RuntimeException while running consumer loop", exception);
        } finally {
            log.info("Closing consumer");
            final CloseOptions closeOptions = this.executionOptions.createCloseOptions(this.consumerConfig);
            this.consumer.close(closeOptions);
            this.shutdownLatch.countDown();
            log.info("Poll loop finished");
        }
    }

    @Override
    public void close() {
        if (!this.running.compareAndSet(true, false)) {
            log.info("Consumer is not running or already stopping");
            return;
        }
        log.info("Gracefully shutting down the consumer");
        this.consumer.wakeup();

        try {
            if (!this.shutdownLatch.await(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                log.warn("Shutdown timed out. Poll loop did not exit cleanly");
            }
        } catch (final InterruptedException e) {
            log.error("Interrupted while waiting for shutdown", e);
            Thread.currentThread().interrupt();
        }
        log.info("Consumer was shut down gracefully");
    }
}
