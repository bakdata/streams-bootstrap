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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.consumer.ConsumerBuilder;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * Provides all runtime configurations when running a {@link ConsumerProducerApp}
 *
 * @see ConsumerProducerApp#buildRunnable(ConsumerProducerBuilder)
 */
@RequiredArgsConstructor
@Value
public class ConsumerProducerBuilder {

    @NonNull
    StreamsTopicConfig topics;
    @NonNull
    ConsumerBuilder consumerBuilder;
    @NonNull
    ProducerBuilder producerBuilder;
    // TODO
//    @NonNull
//    Map<String, Object> producerProperties;
//    @NonNull
//    Map<String, Object> consumerProperties;

//    /**
//     * Create a new {@code Producer} using {@link #producerProperties}
//     * @return {@code Producer}
//     * @param <K> type of keys
//     * @param <V> type of values
//     * @see KafkaProducer#KafkaProducer(Map)
//     */
//    public <K, V> Producer<K, V> createProducer() {
//        return new KafkaProducer<>(this.producerProperties);
//    }
//
//    /**
//     * Create a new {@code Producer} using {@link #producerProperties} and provided {@code Serializers}
//     * @param keySerializer {@code Serializer} to use for keys
//     * @param valueSerializer {@code Serializer} to use for values
//     * @return {@code Producer}
//     * @param <K> type of keys
//     * @param <V> type of values
//     * @see KafkaProducer#KafkaProducer(Map, Serializer, Serializer)
//     */
//    public <K, V> Producer<K, V> createProducer(final Serializer<K> keySerializer,
//            final Serializer<V> valueSerializer) {
//        return new KafkaProducer<>(this.producerProperties, keySerializer, valueSerializer);
//    }
//
//    /**
//     * Create {@code Configurator} to configure {@link org.apache.kafka.common.serialization.Serde} and
//     * {@link Serializer} using {@link #producerProperties}.
//     * @return {@code Configurator}
//     */
//    public Configurator createProducerConfigurator() {
//        return new Configurator(this.producerProperties);
//    }
//
//    /**
//     * Create {@code AppConfiguration} used by this app
//     * @return {@code AppConfiguration}
//     */
//    public AppConfiguration<StreamsTopicConfig> createEffectiveProducerConfiguration() {
//        return new AppConfiguration<>(this.topics, this.producerProperties);
//    }
//
//    /**
//     * Create a new {@code Consumer} using {@link #consumerProperties}
//     * @return {@code Consumer}
//     * @param <K> type of keys
//     * @param <V> type of values
//     * @see KafkaConsumer#KafkaConsumer(Map)
//     */
//    public <K, V> Consumer<K, V> createConsumer() {
//        return new KafkaConsumer<>(this.consumerProperties);
//    }
//
//    /**
//     * Create a new {@code Consumer} using {@link #consumerProperties} and provided {@code Serializers}
//     * @param keyDeserializer {@code Deserializer} to use for keys
//     * @param valueDeserializer {@code Deserializer} to use for values
//     * @return {@code Producer}
//     * @param <K> type of keys
//     * @param <V> type of values
//     * @see KafkaConsumer#KafkaConsumer(Map, Serializer, Serializer)
//     */
//    public <K, V> Consumer<K, V> createConsumer(final Deserializer<K> keyDeserializer,
//            final Deserializer<V> valueDeserializer) {
//        return new KafkaConsumer<>(this.consumerProperties, keyDeserializer, valueDeserializer);
//    }
//
//    /**
//     * Create {@code Configurator} to configure {@link org.apache.kafka.common.serialization.Serde} and
//     * {@link Serializer} using {@link #consumerProperties}.
//     * @return {@code Configurator}
//     */
//    public Configurator createConsumerConfigurator() {
//        return new Configurator(this.consumerProperties);
//    }
//
//    /**
//     * Create {@code EffectiveAppConfiguration} used by this app
//     * @return {@code EffectiveAppConfiguration}
//     */
//    public AppConfiguration<StreamsTopicConfig> createEffectiveConsumerConfiguration() {
//        return new AppConfiguration<>(this.topics, this.consumerProperties);
//    }
}
