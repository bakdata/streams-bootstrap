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

package com.bakdata.kafka.consumerproducer;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class ConfiguredConsumerProducerAppTest {

    private static StreamsAppConfiguration emptyTopicConfig() {
        return new StreamsAppConfiguration(StreamsTopicConfig.builder().build());
    }

    @Test
    void shouldPrioritizeConfigCLIParameters() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), emptyTopicConfig());
        assertThat(configuredApp.getKafkaConsumerProperties(RuntimeConfiguration.create("fake")
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
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(Map.of(
                        "foo", "bar",
                        "hello", "world"
                )), emptyTopicConfig());
        assertThat(configuredApp.getKafkaConsumerProperties(RuntimeConfiguration.create("fake")))
                .containsEntry("foo", "baz")
                .containsEntry("kafka", "streams")
                .containsEntry("hello", "world");
    }

    @Test
    void shouldSetDefaultSerde() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(), emptyTopicConfig());
        assertThat(configuredApp.getKafkaConsumerProperties(RuntimeConfiguration.create("fake")))
                .containsEntry(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .containsEntry(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    }

    @Test
    void shouldThrowIfKeySerdeHasBeenConfiguredDifferently() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(), emptyTopicConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaConsumerProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'key.deserializer' should not be configured already");
    }

    @Test
    void shouldThrowIfValueSerdeHasBeenConfiguredDifferently() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(), emptyTopicConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaConsumerProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'value.deserializer' should not be configured already");
    }

    @Test
    void shouldThrowIfAppIdHasBeenConfiguredDifferently() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(), emptyTopicConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .with(Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG, "my-app"
                ));
        assertThatThrownBy(() -> configuredApp.getKafkaConsumerProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'group.id' should not be configured already");
    }

    @Test
    void shouldThrowIfBootstrapServersHasBeenConfiguredDifferently() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka"
                )), emptyTopicConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaConsumerProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'bootstrap.servers' should not be configured already");
    }

    @Test
    void shouldThrowIfSchemaRegistryHasBeenConfiguredDifferently() {
        final ConfiguredConsumerProducerApp<ConsumerProducerApp> configuredApp =
                new ConfiguredConsumerProducerApp<>(new TestApplication(Map.of(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "my-schema-registry"
                )), emptyTopicConfig());
        final RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.create("fake")
                .withSchemaRegistryUrl("fake");
        assertThatThrownBy(() -> configuredApp.getKafkaConsumerProperties(runtimeConfiguration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'schema.registry.url' should not be configured already");
    }

    // TODO right now overriding createKafkaProperties applies the properties to both consumer and producer
    @RequiredArgsConstructor
    private static class TestApplication implements ConsumerProducerApp {

        private final @NonNull Map<String, Object> kafkaProperties;

        private TestApplication() {
            this(emptyMap());
        }

        @Override
        public Map<String, Object> createKafkaProperties() {
            return this.kafkaProperties;
        }

        @Override
        public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getUniqueAppId(final StreamsAppConfiguration topics) {
            return "app-id";
        }

        @Override
        public SerializerDeserializerConfig defaultSerializationConfig() {
            return new SerializerDeserializerConfig(StringSerializer.class, LongSerializer.class, StringDeserializer.class, LongDeserializer.class);
        }
    }
}
