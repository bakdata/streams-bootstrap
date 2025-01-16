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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

@RequiredArgsConstructor
public class SenderBuilder {

    private final @NonNull Map<String, Object> properties;

    public SenderBuilder() {
        this(new HashMap<>());
    }

    public SenderBuilder with(final String key, final Object value) {
        final Map<String, Object> newProperties = new HashMap<>(this.properties);
        newProperties.put(key, value);
        return new SenderBuilder(Map.copyOf(newProperties));
    }

    public <K, V> void to(final String topic, final Iterable<SimpleProducerRecord<K, V>> records) {
        try (final Producer<K, V> producer = new KafkaProducer<>(this.properties)) {
            records.forEach(kv -> producer.send(kv.toProducerRecord(topic)));
        }
    }

    @Value
    @RequiredArgsConstructor
    public static class SimpleProducerRecord<K, V> {
        K key;
        V value;
        Instant timestamp;
        Iterable<Header> headers;

        public SimpleProducerRecord(final K key, final V value) {
            this(key, value, (Instant) null);
        }

        public SimpleProducerRecord(final K key, final V value, final Instant timestamp) {
            this(key, value, timestamp, null);
        }

        public SimpleProducerRecord(final K key, final V value, final Iterable<Header> headers) {
            this(key, value, null, headers);
        }

        private ProducerRecord<K, V> toProducerRecord(final String topic) {
            return new ProducerRecord<>(topic, null, this.timestamp.toEpochMilli(), this.key, this.value, this.headers);
        }
    }

}
