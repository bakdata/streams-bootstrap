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
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.producer.ConfiguredProducerApp;
import com.bakdata.kafka.producer.ProducerApp;
import com.bakdata.kafka.producer.ProducerAppConfiguration;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.producer.ProducerRunnable;
import com.bakdata.kafka.producer.ProducerTopicConfig;
import com.bakdata.kafka.producer.SerializerConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class ConfiguredProducerAppTest {

    private static ProducerTopicConfig emptyTopicConfig() {
        return ProducerTopicConfig.builder().build();
    }

    private static ProducerAppConfiguration newAppConfiguration() {
        return new ProducerAppConfiguration(emptyTopicConfig());
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(Map.of(
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
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), newAppConfiguration());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultSerializer() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), newAppConfiguration());
        assertThat(configuredApp.getKafkaProperties(RuntimeConfiguration.create("fake")))
                .containsEntry(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .containsEntry(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
    }

    @Test
    void shouldThrowIfKeySerializerHasBeenConfiguredDifferently() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'key.serializer' should not be configured already");
    }

    @Test
    void shouldThrowIfValueSerializerHasBeenConfiguredDifferently() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'value.serializer' should not be configured already");
    }

    @Test
    void shouldThrowIfBootstrapServersHasBeenConfiguredDifferently() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka"
                )), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'bootstrap.servers' should not be configured already");
    }

    @Test
    void shouldThrowIfSchemaRegistryHasBeenConfiguredDifferently() {
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "my-schema-registry"
                )), newAppConfiguration());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .withSchemaRegistryUrl("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'schema.registry.url' should not be configured already");
    }

    @RequiredArgsConstructor
    private static class TestProducer implements ProducerApp {

        private final @NonNull Map<String, Object> kafkaProperties;

        private TestProducer() {
            this(emptyMap());
        }

        @Override
        public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> createKafkaProperties() {
            return this.kafkaProperties;
        }

        @Override
        public SerializerConfig defaultSerializationConfig() {
            return new SerializerConfig(StringSerializer.class, LongSerializer.class);
        }
    }
}
