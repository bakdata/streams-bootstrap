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

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("System exit stops Kafka Streams running in sub-thread")
class CliTest {

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        KafkaStreamsApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public void buildTopology(final StreamsBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--brokers", "dummy",
                "--schema-registry-url", "dummy",
                "--input-topics", "input",
                "--output-topic", "output"});
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCode() {
        KafkaStreamsApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public void buildTopology(final StreamsBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void run() {
                throw new RuntimeException();
            }
        }, new String[]{
                "--brokers", "dummy",
                "--schema-registry-url", "dummy",
                "--input-topics", "input",
                "--output-topic", "output"});
    }
}
