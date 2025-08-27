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

import com.bakdata.kafka.SerializerDeserializerConfig;
import com.bakdata.kafka.consumerproducer.ConsumerProducerApp;
import com.bakdata.kafka.consumerproducer.ConsumerProducerBuilder;
import com.bakdata.kafka.consumerproducer.ConsumerProducerRunnable;
import com.bakdata.kafka.consumerproducer.KafkaConsumerProducerApplication;
import com.bakdata.kafka.streams.KafkaStreamsApplication;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.kstream.KStreamX;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import java.time.Duration;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@NoArgsConstructor
@Getter
@Setter
public class CloseFlagApp extends KafkaConsumerProducerApplication<ConsumerProducerApp> {
    private boolean closed = false;
    private boolean appClosed = false;

    @Override
    public void close() {
        super.close();
        this.closed = true;
    }

    @Override
    public ConsumerProducerApp createApp() {
        return new ConsumerProducerApp() {

            @Override
            public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                return () -> {
                    try (final Consumer<String, String> consumer = builder.getConsumerBuilder().createConsumer();
                            final Producer<String, String> producer = builder.getProducerBuilder().createProducer()) {
                        this.initConsumerProducer(consumer, producer, builder);
                    }
                };
            }

            private void initConsumerProducer(final Consumer<String, String> consumer, final Producer<String, String> producer, final ConsumerProducerBuilder builder) {
                consumer.subscribe(builder.getTopics().getInputTopics());
                while (!CloseFlagApp.this.appClosed) {
                    final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100L));
                    consumerRecords.forEach(record -> producer.send(
                            new ProducerRecord<>(builder.getTopics().getOutputTopic(), record.key(), record.value())));
                }
            }

            @Override
            public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                return CloseFlagApp.this.getClass().getSimpleName() + "-" + configuration.getTopics().getOutputTopic();
            }

            @Override
            public SerializerDeserializerConfig defaultSerializationConfig() {
                return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class, StringDeserializer.class, StringDeserializer.class);
            }

            @Override
            public void close() {
                CloseFlagApp.this.appClosed = true;
            }
        };
    }
}
