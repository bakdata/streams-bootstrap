/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

class PropertiesTest {

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final TestApplication app = new TestApplication();
        KafkaApplication.startApplicationWithoutExit(app,
                new String[]{
                        "--brokers", "fake",
                        "--schema-registry-url", "fake",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic",
                        "--streams-config", "foo=baz",
                        "--streams-config", "kafka=streams"
                });
        assertThat(app.getKafkaProperties())
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultAvroSerdeWhenSchemaRegistryUrlIsSet() {
        final TestApplication app = new TestApplication();
        KafkaApplication.startApplicationWithoutExit(app,
                new String[]{
                        "--brokers", "fake",
                        "--schema-registry-url", "fake",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic"
                });
        assertThat(app.getKafkaProperties())
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Test
    void shouldSetDefaultStringSerdeWhenSchemaRegistryUrlIsNotSet() {
        final TestApplication app = new TestApplication();
        KafkaApplication.startApplicationWithoutExit(app,
                new String[]{
                        "--brokers", "fake",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic"
                });
        assertThat(app.getKafkaProperties())
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
    }

    @Test
    void shouldSetDefaultAvroSerializerWhenSchemaRegistryUrlIsSet() {
        final TestProducer app = new TestProducer();
        KafkaApplication.startApplicationWithoutExit(app,
                new String[]{
                        "--brokers", "fake",
                        "--schema-registry-url", "fake",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic"
                });
        assertThat(app.getKafkaProperties())
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
    }

    @Test
    void shouldSetDefaultStringSerializerWhenSchemaRegistryUrlIsNotSet() {
        final TestProducer app = new TestProducer();
        KafkaApplication.startApplicationWithoutExit(app,
                new String[]{
                        "--brokers", "fake",
                        "--output-topic", "output",
                        "--input-topics", "input",
                        "--error-topic", "error-topic"
                });
        assertThat(app.getKafkaProperties())
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    private static class TestApplication extends KafkaStreamsApplication {

        @Override
        public void buildTopology(final StreamsBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void run() {
            // do nothing
        }

        @Override
        public String getUniqueAppId() {
            return "foo";
        }

        @Override
        protected Properties createKafkaProperties() {
            final Properties properties = super.createKafkaProperties();
            properties.setProperty("foo", "bar");
            properties.setProperty("hello", "world");
            return properties;
        }
    }

    private static class TestProducer extends KafkaProducerApplication {

        @Override
        protected void runApplication() {
            // do noting
        }
    }
}
