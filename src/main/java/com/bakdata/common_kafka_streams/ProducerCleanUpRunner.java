/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

package com.bakdata.common_kafka_streams;

import static com.bakdata.common_kafka_streams.KafkaApplication.RESET_SLEEP_MS;

import java.util.Properties;
import lombok.Builder;
import lombok.NonNull;

public class ProducerCleanUpRunner extends CleanUpRunner {
    @Builder
    public ProducerCleanUpRunner(@NonNull final Properties kafkaProperties,
            @NonNull final String schemaRegistryUrl, @NonNull final String brokers) {
        super(kafkaProperties, schemaRegistryUrl, brokers);
    }

    public void run(final Iterable<String> outputTopics) {
        outputTopics.forEach(this::run);
    }

    private void run(final String outputTopic) {
        this.deleteTopicAndResetSchemaRegistry(outputTopic);
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error waiting for clean up", e);
        }
    }
}
