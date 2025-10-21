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

package com.bakdata.kafka.consumerproducer.apps;

import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.consumerproducer.ConsumerProducerApp;
import com.bakdata.kafka.consumerproducer.ConsumerProducerAppConfiguration;
import com.bakdata.kafka.consumerproducer.ConsumerProducerBuilder;
import com.bakdata.kafka.consumerproducer.ConsumerProducerRunnable;
import com.bakdata.kafka.consumerproducer.SerializerDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
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
public class MirrorKeyWithAvroConsumerProducer implements ConsumerProducerApp {

    private final AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public SerializerDeserializerConfig defaultSerializationConfig() {
        return new SerializerDeserializerConfig(SpecificAvroSerializer.class, StringSerializer.class,
                SpecificAvroDeserializer.class, StringDeserializer.class);
    }

    @Override
    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
        return () -> {
            try (final Consumer<TestRecord, String> consumer = builder.consumerBuilder().createConsumer();
                    final Producer<TestRecord, String> producer = builder.producerBuilder().createProducer()) {
                this.initConsumer(consumer, producer, builder);
            }
        };
    }

    @Override
    public String getUniqueAppId(final ConsumerProducerAppConfiguration topics) {
        return "app-id";
    }

    private void initConsumer(final Consumer<TestRecord, String> consumer, final Producer<TestRecord, String> producer,
            final ConsumerProducerBuilder builder) {
        consumer.subscribe(builder.topics().getInputTopics());
        while (this.running.get()) {
            final ConsumerRecords<TestRecord, String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
            consumerRecords.forEach(consumerRecord -> producer.send(
                    new ProducerRecord<>(builder.topics().getOutputTopic(), consumerRecord.key(), consumerRecord.value())));
        }
    }

    public void shutdown() {
        this.running.set(false);
    }

    public void start() {
        this.running.set(true);
    }
}
