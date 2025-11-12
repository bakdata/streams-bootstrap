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

import com.bakdata.kafka.consumer.ConsumerRunnable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

@AllArgsConstructor
@Slf4j
public class DefaultConsumerProducerRunnable<KOut, VOut> implements ConsumerProducerRunnable {

    private final Producer<KOut, VOut> producer;
    @Getter
    private final ConsumerRunnable consumerRunnable;

    @Override
    public void run(final ConsumerConfig consumerConfig, final ProducerConfig producerConfig) {
        this.consumerRunnable.run(consumerConfig);
    }

    @Override
    public void close() {
        try {
            log.debug("Closing consumer runnable");
            this.consumerRunnable.close();
        } catch (final RuntimeException e) {
            log.warn("Error closing consumer runnable", e);
        }

        try {
            log.debug("Closing producer");
            this.producer.close();
        } catch (final RuntimeException e) {
            log.warn("Error closing producer", e);
        }

        log.info("ConsumerProducer was shut down gracefully");
    }
}
