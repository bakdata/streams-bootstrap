/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Map;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class ConfiguredProducerAppTest {

    private static AppConfiguration<ProducerTopicConfig> newAppConfiguration() {
        return new AppConfiguration<>(emptyTopicConfig());
    }

    private static ProducerTopicConfig emptyTopicConfig() {
        return ProducerTopicConfig.builder().build();
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final AppConfiguration<ProducerTopicConfig> configuration = new AppConfiguration<>(emptyTopicConfig(), Map.of(
                "foo", "baz",
                "kafka", "streams"
        ));
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .bootstrapServers("fake")
                .build()))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    @SetEnvironmentVariable(key = "STREAMS_FOO", value = "baz")
    @SetEnvironmentVariable(key = "STREAMS_STREAMS", value = "streams")
    void shouldPrioritizeEnvironmentConfigs() {
        final AppConfiguration<ProducerTopicConfig> configuration = newAppConfiguration();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .bootstrapServers("fake")
                .build()))
                .containsEntry("foo", "baz")
                .containsEntry("streams", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultAvroSerializerWhenSchemaRegistryUrlIsSet() {
        final AppConfiguration<ProducerTopicConfig> configuration = newAppConfiguration();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .bootstrapServers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
    }

    @Test
    void shouldSetDefaultStringSerializerWhenSchemaRegistryUrlIsNotSet() {
        final AppConfiguration<ProducerTopicConfig> configuration = newAppConfiguration();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .bootstrapServers("fake")
                .build()))
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    private static class TestProducer implements ProducerApp {

        @Override
        public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> createKafkaProperties() {
            return Map.of(
                    "foo", "bar",
                    "hello", "world"
            );
        }
    }
}
