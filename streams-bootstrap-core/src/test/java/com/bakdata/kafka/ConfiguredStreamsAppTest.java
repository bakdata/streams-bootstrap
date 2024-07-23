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

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class ConfiguredStreamsAppTest {

    private static StreamsTopicConfig emptyTopicConfig() {
        return StreamsTopicConfig.builder().build();
    }

    private static AppConfiguration<StreamsTopicConfig> newAppConfiguration() {
        return new AppConfiguration<>(emptyTopicConfig());
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(emptyTopicConfig(), Map.of(
                "foo", "baz",
                "kafka", "streams"
        ));
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .build()))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    @SetEnvironmentVariable(key = "STREAMS_FOO", value = "baz")
    @SetEnvironmentVariable(key = "STREAMS_STREAMS", value = "streams")
    void shouldPrioritizeEnvironmentConfigs() {
        final AppConfiguration<StreamsTopicConfig> configuration = newAppConfiguration();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .build()))
                .containsEntry("foo", "baz")
                .containsEntry("streams", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultSerde() {
        final AppConfiguration<StreamsTopicConfig> configuration = newAppConfiguration();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThat(configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, LongSerde.class);
    }

    @Test
    void shouldThrowIfKeySerdeHasBeenConfiguredDifferently() {
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(emptyTopicConfig(), Map.of(
                DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class
        ));
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'default.key.serde' should not be configured already");
    }

    @Test
    void shouldThrowIfValueSerdeHasBeenConfiguredDifferently() {
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(emptyTopicConfig(), Map.of(
                DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class
        ));
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'default.value.serde' should not be configured already");
    }

    @Test
    void shouldThrowIfAppIdHasBeenConfiguredDifferently() {
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(emptyTopicConfig(), Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "my-app"
        ));
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), configuration);
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(KafkaEndpointConfig.builder()
                .brokers("fake")
                .schemaRegistryUrl("fake")
                .build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'application.id' should not be configured already");
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
        public Map<String, Object> createKafkaProperties() {
            return Map.of(
                    "foo", "bar",
                    "hello", "world"
            );
        }

        @Override
        public SerdeConfig defaultSerializationConfig() {
            return new SerdeConfig(StringSerde.class, LongSerde.class);
        }
    }
}
