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

package com.bakdata.kafka.streams.kstream;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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

    private static StreamsAppConfiguration newAppConfiguration() {
        return new StreamsAppConfiguration(emptyTopicConfig());
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), newAppConfiguration());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")
                .with(Map.of(
                        "foo", "baz",
                        "kafka", "streams"
                ))))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_FOO", value = "baz")
    @SetEnvironmentVariable(key = "KAFKA_KAFKA", value = "streams")
    void shouldPrioritizeEnvironmentConfigs() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), newAppConfiguration());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultSerde() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), newAppConfiguration());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class)
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, LongSerde.class);
    }

    @Test
    void shouldThrowIfKeySerdeHasBeenConfiguredDifferently() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        DEFAULT_KEY_SERDE_CLASS_CONFIG, ByteArraySerde.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'default.key.serde' should not be configured already");
    }

    @Test
    void shouldThrowIfValueSerdeHasBeenConfiguredDifferently() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        DEFAULT_VALUE_SERDE_CLASS_CONFIG, ByteArraySerde.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'default.value.serde' should not be configured already");
    }

    @Test
    void shouldThrowIfAppIdHasBeenConfiguredDifferently() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "my-app"
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'application.id' should not be configured already");
    }

    @Test
    void shouldThrowIfBootstrapServersHasBeenConfiguredDifferently() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(Map.of(
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka"
                )), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'bootstrap.servers' should not be configured already");
    }

    @Test
    void shouldThrowIfSchemaRegistryHasBeenConfiguredDifferently() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "my-schema-registry"
                )), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake"
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'schema.registry.url' should not be configured already");
    }

    @Test
    void shouldThrowIfAppIdIsInconsistent() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp = new ConfiguredStreamsApp<>(new StreamsApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                return "foo";
            }
        }, new StreamsAppConfiguration(emptyTopicConfig(), "not_foo"));
        assertThatThrownBy(configuredApp::getUniqueAppId)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Provided application ID does not match StreamsApp#getUniqueAppId()");
    }

    @Test
    void shouldThrowIfAppIdIsNull() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp = new ConfiguredStreamsApp<>(new StreamsApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                return null;
            }
        }, new StreamsAppConfiguration(emptyTopicConfig(), "foo"));
        assertThatThrownBy(configuredApp::getUniqueAppId)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Application ID cannot be null");
    }

    @Test
    void shouldReturnConfiguredAppId() {
        final ConfiguredStreamsApp<StreamsApp> configuredApp = new ConfiguredStreamsApp<>(new StreamsApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }
        }, new StreamsAppConfiguration(emptyTopicConfig(), "foo"));
        assertThat(configuredApp.getUniqueAppId()).isEqualTo("foo");
    }

    @RequiredArgsConstructor
    private static class TestApplication implements StreamsApp {

        private final @NonNull Map<String, Object> kafkaProperties;

        private TestApplication() {
            this(emptyMap());
        }

        @Override
        public void buildTopology(final StreamsBuilderX builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getUniqueAppId(final StreamsAppConfiguration configuration) {
            return "foo";
        }

        @Override
        public Map<String, Object> createKafkaProperties() {
            return this.kafkaProperties;
        }

        @Override
        public SerdeConfig defaultSerializationConfig() {
            return new SerdeConfig(StringSerde.class, LongSerde.class);
        }
    }
}
