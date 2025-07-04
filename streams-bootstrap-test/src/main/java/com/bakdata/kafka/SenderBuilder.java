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

package com.bakdata.kafka;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Send data to a Kafka cluster
 * @param <K> type of keys serialized by the reader
 * @param <V> type of values serialized by the reader
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class SenderBuilder<K, V> {

    private final @NonNull Map<String, Object> properties;
    private final @NonNull Preconfigured<Serializer<K>> keySerializer;
    private final @NonNull Preconfigured<Serializer<V>> valueSerializer;

    static <K, V> SenderBuilder<K, V> create(final Map<String, Object> properties) {
        return new SenderBuilder<>(properties, Preconfigured.defaultSerializer(), Preconfigured.defaultSerializer());
    }

    /**
     * Add a producer configuration
     *
     * @param key configuration key
     * @param value configuration value
     * @return {@code SenderBuilder} with added configuration
     */
    public SenderBuilder<K, V> with(final String key, final Object value) {
        final Map<String, Object> newProperties = new HashMap<>(this.properties);
        newProperties.put(key, value);
        return new SenderBuilder<>(Map.copyOf(newProperties), this.keySerializer, this.valueSerializer);
    }

    /**
     * Send data to a topic
     *
     * @param topic topic to send to
     * @param records records to send
     */
    public void to(final String topic, final Iterable<SimpleProducerRecord<K, V>> records) {
        try (final Producer<K, V> producer = this.createProducer()) {
            records.forEach(kv -> producer.send(kv.toProducerRecord(topic)));
        }
    }

    /**
     * Create a new {@code Producer} for a Kafka cluster
     *
     * @return {@code Producer}
     */
    public Producer<K, V> createProducer() {
        return new KafkaProducer<>(this.properties, this.keySerializer.configureForKeys(this.properties),
                this.valueSerializer.configureForValues(this.properties));
    }

    /**
     * Provide custom serializers for keys and values. Serializers are configured automatically.
     *
     * @param keySerializer serializer for keys
     * @param valueSerializer serializer for values
     * @param <KN> type of keys
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom serializers
     */
    public <KN, VN> SenderBuilder<KN, VN> withSerializers(final Preconfigured<Serializer<KN>> keySerializer,
            final Preconfigured<Serializer<VN>> valueSerializer) {
        return new SenderBuilder<>(this.properties, keySerializer, valueSerializer);
    }

    /**
     * Provide custom serializers for keys and values. Serializers are configured automatically.
     * @param keySerializer serializer for keys
     * @param valueSerializer serializer for values
     * @param <KN> type of keys
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom serializers
     * @see SenderBuilder#withSerializers(Preconfigured, Preconfigured)
     */
    public <KN, VN> SenderBuilder<KN, VN> withSerializers(final Serializer<KN> keySerializer,
            final Serializer<VN> valueSerializer) {
        return this.withSerializers(Preconfigured.create(keySerializer), Preconfigured.create(valueSerializer));
    }

    /**
     * Provide a custom serializers for keys. Serializer is configured automatically.
     * @param keySerializer serializer for keys
     * @param <KN> type of keys
     * @return {@code SenderBuilder} with custom key serializer
     */
    public <KN> SenderBuilder<KN, V> withKeySerializer(final Preconfigured<Serializer<KN>> keySerializer) {
        return this.withSerializers(keySerializer, this.valueSerializer);
    }

    /**
     * Provide a custom serializers for keys. Serializer is configured automatically.
     * @param keySerializer serializer for keys
     * @param <KN> type of keys
     * @return {@code SenderBuilder} with custom key serializer
     * @see SenderBuilder#withKeySerializer(Preconfigured)
     */
    public <KN> SenderBuilder<KN, V> withKeySerializer(final Serializer<KN> keySerializer) {
        return this.withKeySerializer(Preconfigured.create(keySerializer));
    }

    /**
     * Provide a custom serializers for values. Serializer is configured automatically.
     * @param valueSerializer serializer for values
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom value serializer
     */
    public <VN> SenderBuilder<K, VN> withValueSerializer(final Preconfigured<Serializer<VN>> valueSerializer) {
        return this.withSerializers(this.keySerializer, valueSerializer);
    }

    /**
     * Provide a custom serializers for values. Serializer is configured automatically.
     * @param valueSerializer serializer for values
     * @param <VN> type of values
     * @return {@code SenderBuilder} with custom value serializer
     * @see SenderBuilder#withValueSerializer(Preconfigured)
     */
    public <VN> SenderBuilder<K, VN> withValueSerializer(final Serializer<VN> valueSerializer) {
        return this.withValueSerializer(Preconfigured.create(valueSerializer));
    }

    /**
     * Represents a {@link ProducerRecord} without topic assignment
     *
     * @param <K> type of keys
     * @param <V> type of values
     */
    @Value
    @RequiredArgsConstructor
    public static class SimpleProducerRecord<K, V> {
        K key;
        V value;
        Instant timestamp;
        Iterable<Header> headers;

        /**
         * Create a new {@code SimpleProducerRecord} without timestamp and headers
         *
         * @param key key
         * @param value value
         */
        public SimpleProducerRecord(final K key, final V value) {
            this(key, value, (Instant) null);
        }

        /**
         * Create a new {@code SimpleProducerRecord} without headers
         *
         * @param key key
         * @param value value
         * @param timestamp timestamp
         */
        public SimpleProducerRecord(final K key, final V value, final Instant timestamp) {
            this(key, value, timestamp, null);
        }

        /**
         * Create a new {@code SimpleProducerRecord} without timestamp
         *
         * @param key key
         * @param value value
         * @param headers headers
         */
        public SimpleProducerRecord(final K key, final V value, final Iterable<Header> headers) {
            this(key, value, null, headers);
        }

        private ProducerRecord<K, V> toProducerRecord(final String topic) {
            final Long timestampMillis = this.timestamp == null ? null : this.timestamp.toEpochMilli();
            return new ProducerRecord<>(topic, null, timestampMillis, this.key, this.value, this.headers);
        }
    }

}
