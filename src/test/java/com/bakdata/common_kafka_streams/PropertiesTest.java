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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

class PropertiesTest {

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final TestApplication app = new TestApplication();
        KafkaStreamsApplication.startApplication(app,
                new String[]{"--brokers", "dummy",
                        "--schema-registry-url", "dummy",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic",
                        "--streams-config", "foo=baz",
                        "--streams-config", "kafka=streams"});
        assertThat(app.getKafkaProperties())
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    private static class TestApplication extends KafkaStreamsApplication {

        @Override
        public void buildTopology(final StreamsBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void run() {
            //do nothing
        }

        @Override
        public String getUniqueAppId() {
            return "foo";
        }

        @Override
        protected Properties createKafkaProperties() {
            final Properties properties = new Properties();
            properties.setProperty("foo", "bar");
            properties.setProperty("hello", "world");
            return properties;
        }
    }
}
