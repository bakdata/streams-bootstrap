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

package com.bakdata.kafka.test_applications;

import com.bakdata.kafka.ConsumerProducerApp;
import com.bakdata.kafka.ConsumerProducerBuilder;
import com.bakdata.kafka.ConsumerProducerRunnable;
import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.SerdeConfig;
import com.bakdata.kafka.SerializerConfig;
import com.bakdata.kafka.SerializerDeserializerConfig;
import com.bakdata.kafka.StreamsTopicConfig;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Getter
@RequiredArgsConstructor
public class StringConsumerProducer implements ConsumerProducerApp {

    // TODO necessary?
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public SerializerDeserializerConfig defaultSerializationConfig() {
        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class, StringDeserializer.class, StringDeserializer.class);
    }

    @Override
    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
        return () -> {
            try (final Consumer<String, String> consumer = builder.getConsumerBuilder().createConsumer();
                    final Producer<String, String> producer = builder.getProducerBuilder().createProducer()) {
                this.initConsumer(consumer, producer, builder);
            }
        };
    }

    @Override
    public String getUniqueAppId(final StreamsTopicConfig topics) {
        return "app-id";
    }

    private void initConsumer(final Consumer<String, String> consumer, final Producer<String, String> producer, final ConsumerProducerBuilder builder) {
        consumer.subscribe(builder.getTopics().getInputTopics());
        while (this.running.get()) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
            consumerRecords.forEach(record -> producer.send(
                    new ProducerRecord<>(builder.getTopics().getOutputTopic(), record.key(), record.value())));
        }
    }

    public void shutdown() {
        this.running.set(false);
    }

    public void start() {
        this.running.set(true);
    }
}
