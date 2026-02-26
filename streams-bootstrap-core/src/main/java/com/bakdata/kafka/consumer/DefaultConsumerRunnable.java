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

package com.bakdata.kafka.consumer;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Default implementation of {@link ConsumerRunnable} that manages the Kafka consumer poll loop and record processing
 * lifecycle. This class handles the consumer poll loop, automatic offset commits, and graceful shutdown. It delegates
 * record processing to the provided {@link java.util.function.Consumer}.
 *
 * @param <K> type of keys
 * @param <V> type of values
 */
@RequiredArgsConstructor
@Slf4j
public class DefaultConsumerRunnable<K, V> implements ConsumerRunnable {

    private final Consumer<K, V> consumer;
    /**
     * The processor that implements the logic for handling each batch of
     * {@link ConsumerRecords} polled from Kafka.
     */
    private final java.util.function.Consumer<ConsumerRecords<K, V>> recordProcessor;

    @Override
    public void run(final Duration pollTimeout) {
        final ConsumerRecords<K, V> consumerRecords = this.consumer.poll(pollTimeout);
        if (!consumerRecords.isEmpty()) {
            log.debug("Polled {} records", consumerRecords.count());
            this.recordProcessor.accept(consumerRecords);
            this.consumer.commitSync();
        }
    }

    @Override
    public void close(final CloseOptions closeOptions) {
        log.info("Gracefully shutting down the consumer");
        this.consumer.close(closeOptions);
    }

    @Override
    public void wakeup() {
        this.consumer.wakeup();
    }
}
