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

package com.bakdata.kafka.consumer;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.RuntimeConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class ConfiguredConsumerAppTest {

    private static ConsumerAppConfiguration emptyAppConfig() {
        return new ConsumerAppConfiguration(ConsumerTopicConfig.builder().build());
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), emptyAppConfig());
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
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), emptyAppConfig());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultSerializer() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), emptyAppConfig());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .containsEntry(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    }

    @Test
    void shouldThrowIfKeySerializerHasBeenConfiguredDifferently() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), emptyAppConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'key.deserializer' should not be configured already");
    }

    @Test
    void shouldThrowIfValueSerializerHasBeenConfiguredDifferently() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), emptyAppConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'value.deserializer' should not be configured already");
    }

    @Test
    void shouldThrowIfBootstrapServersHasBeenConfiguredDifferently() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka"
                )), emptyAppConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'bootstrap.servers' should not be configured already");
    }

    @Test
    void shouldThrowIfSchemaRegistryHasBeenConfiguredDifferently() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "my-schema-registry"
                )), emptyAppConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .withSchemaRegistryUrl("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'schema.registry.url' should not be configured already");
    }

    @Test
    void shouldThrowIfAppIdIsInconsistent() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp = new ConfiguredConsumerApp<>(new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return "foo";
            }
        }, new ConsumerAppConfiguration(emptyAppConfig().getTopics(), "not_foo"));
        assertThatThrownBy(configuredApp::getUniqueGroupId)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Provided group ID does not match ConsumerApp#getUniqueGroupId()");
    }

    @Test
    void shouldThrowIfAppIdIsNull() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp = new ConfiguredConsumerApp<>(new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                return null;
            }
        }, new ConsumerAppConfiguration(emptyAppConfig().getTopics(), "foo"));
        assertThatThrownBy(configuredApp::getUniqueGroupId)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Group ID cannot be null");
    }

    @Test
    void shouldReturnConfiguredAppId() {
        final ConfiguredConsumerApp<ConsumerApp> configuredApp = new ConfiguredConsumerApp<>(new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }
        }, new ConsumerAppConfiguration(emptyAppConfig().getTopics(), "foo"));
        assertThat(configuredApp.getUniqueGroupId()).isEqualTo("foo");
    }

    private record TestConsumer(@NonNull Map<String, Object> kafkaProperties) implements ConsumerApp {

        private TestConsumer() {
            this(emptyMap());
        }

        @Override
        public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getUniqueGroupId(final ConsumerAppConfiguration config) {
            return "app-id";
        }

        @Override
        public Map<String, Object> createKafkaProperties() {
            return this.kafkaProperties;
        }

        @Override
        public DeserializerConfig defaultSerializationConfig() {
            return new DeserializerConfig(StringDeserializer.class, LongDeserializer.class);
        }
    }
}
