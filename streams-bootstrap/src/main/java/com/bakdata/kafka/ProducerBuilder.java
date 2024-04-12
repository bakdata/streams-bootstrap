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

import static java.util.Collections.emptyMap;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Provides all runtime configurations when running a {@link ProducerApp}
 *
 * @see ProducerApp#buildRunnable(ProducerBuilder)
 */
@Builder(access = AccessLevel.PACKAGE)
@Value
public class ProducerBuilder {

    @NonNull ProducerTopicConfig topics;
    @NonNull Map<String, Object> kafkaProperties;

    /**
     * Create a new {@code Producer} using {@link #kafkaProperties}
     * @return {@code Producer}
     * @param <K> type of keys
     * @param <V> type of values
     * @see KafkaProducer#KafkaProducer(Map)
     */
    public <K, V> Producer<K, V> createProducer() {
        return new KafkaProducer<>(this.kafkaProperties);
    }

    /**
     * Create a new {@code Producer} using {@link #kafkaProperties} and provided {@code Serializers}
     * @param keySerializer {@code Serializer} to use for keys
     * @param valueSerializer {@code Serializer} to use for values
     * @return {@code Producer}
     * @param <K> type of keys
     * @param <V> type of values
     * @see KafkaProducer#KafkaProducer(Map, Serializer, Serializer)
     */
    public <K, V> Producer<K, V> createProducer(final Serializer<K> keySerializer,
            final Serializer<V> valueSerializer) {
        return new KafkaProducer<>(this.kafkaProperties, keySerializer, valueSerializer);
    }

    /**
     * Configure a {@code Serializer} for values using {@link #kafkaProperties}
     * @param serializer serializer to configure
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureValueSerializer(final Serializer<T> serializer) {
        return this.configureValueSerializer(serializer, emptyMap());
    }

    /**
     * Configure a {@code Serializer} for values using {@link #kafkaProperties} and config overrides
     * @param serializer serializer to configure
     * @param config configuration overrides
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureValueSerializer(final Serializer<T> serializer,
            final Map<String, Object> config) {
        final Map<String, Object> serializerConfig = this.mergeConfig(config);
        serializer.configure(serializerConfig, false);
        return serializer;
    }

    /**
     * Configure a {@code Serializer} for keys using {@link #kafkaProperties}
     * @param serializer serializer to configure
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureKeySerializer(final Serializer<T> serializer) {
        return this.configureKeySerializer(serializer, emptyMap());
    }

    /**
     * Configure a {@code Serializer} for keys using {@link #kafkaProperties} and config overrides
     * @param serializer serializer to configure
     * @param config configuration overrides
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureKeySerializer(final Serializer<T> serializer, final Map<String, Object> config) {
        final Map<String, Object> serializerConfig = this.mergeConfig(config);
        serializer.configure(serializerConfig, true);
        return serializer;
    }

    private Map<String, Object> mergeConfig(final Map<String, Object> config) {
        return ImmutableMap.<String, Object>builder()
                .putAll(this.kafkaProperties)
                .putAll(config)
                .build();
    }
}
