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

package com.bakdata.kafka.streams.apps;

import com.bakdata.kafka.consumer.ConsumerRunnable;
import com.bakdata.kafka.consumerproducer.ConsumerProducerApp;
import com.bakdata.kafka.consumerproducer.ConsumerProducerAppConfiguration;
import com.bakdata.kafka.consumerproducer.ConsumerProducerBuilder;
import com.bakdata.kafka.consumerproducer.ConsumerProducerRunnable;
import com.bakdata.kafka.consumerproducer.DefaultConsumerProducerRunnable;
import com.bakdata.kafka.consumerproducer.SerializerDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleConsumerProducerApp implements ConsumerProducerApp {

    @Override
    public SerializerDeserializerConfig defaultSerializationConfig() {
        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class,
                StringDeserializer.class, StringDeserializer.class);
    }

    @Override
    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
        final Producer<String, String> producer = builder.producerBuilder().createProducer();
        final Consumer<String, String> consumer = builder.consumerBuilder().createConsumer();
        builder.consumerBuilder().subscribeToAllTopics(consumer);
        final ConsumerRunnable
                consumerRunnable = builder.consumerBuilder().createDefaultConsumerRunnable(consumer, records ->
                records.forEach(consumerRecord ->
                        producer.send(new ProducerRecord<>(builder.topics().getOutputTopic(),
                                consumerRecord.key(), consumerRecord.value()))));
        return new DefaultConsumerProducerRunnable<>(producer, consumerRunnable);
    }

    @Override
    public String getUniqueAppId(final ConsumerProducerAppConfiguration topics) {
        return "app-id";
    }
}
