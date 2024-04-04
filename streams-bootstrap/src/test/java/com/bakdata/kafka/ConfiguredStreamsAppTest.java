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

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.junit.jupiter.api.Test;

class ConfiguredStreamsAppTest {

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), StreamsAppConfiguration.builder()
                        .kafkaConfig(Map.of(
                                "foo", "baz",
                                "kafka", "streams"
                        ))
                        .build());
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .build()))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultAvroSerdeWhenSchemaRegistryUrlIsSet() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), StreamsAppConfiguration.builder()
                        .build());
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class)
                .containsEntry(SCHEMA_REGISTRY_URL_CONFIG, "fake");
    }

    @Test
    void shouldSetDefaultStringSerdeWhenSchemaRegistryUrlIsNotSet() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), StreamsAppConfiguration.builder()
                        .build());
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .build()))
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
    }

    private static class TestApplication implements StreamsApp {

        @Override
        public void buildTopology(final TopologyBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getUniqueAppId(final StreamsTopicConfig topics) {
            return "foo";
        }

        @Override
        public Map<String, Object> createKafkaProperties(final StreamsConfigurationOptions options) {
            final Map<String, Object> properties = StreamsApp.super.createKafkaProperties(options);
            properties.put("foo", "bar");
            properties.put("hello", "world");
            return properties;
        }
    }
}
