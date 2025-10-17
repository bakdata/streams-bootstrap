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

import com.bakdata.kafka.consumerproducer.SerializerDeserializerConfig;
import com.bakdata.kafka.consumerproducer.ConsumerProducerApp;
import com.bakdata.kafka.consumerproducer.ConsumerProducerBuilder;
import com.bakdata.kafka.consumerproducer.ConsumerProducerRunnable;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import java.time.Duration;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

@NoArgsConstructor
public class Mirror implements ConsumerProducerApp {

    @Override
    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
        return new ConsumerProducerRunnable() {
            private Consumer<String, String> consumer = null;
            private Producer<String, String> producer = null;
            private volatile boolean running = true;

            @Override
            public void close() {
                this.running = false;
                this.consumer.wakeup();
            }

            @Override
            public void run() {
                try {
                    this.consumer = builder.consumerBuilder().createConsumer();
                    this.consumer.subscribe(builder.topics().getInputTopics());
                    this.producer = builder.producerBuilder().createProducer();
                    while (this.running) {
                        final ConsumerRecords<String, String> consumerRecords =
                                this.consumer.poll(Duration.ofMillis(100L));
                        consumerRecords.forEach(record -> this.producer.send(
                                new ProducerRecord<>(builder.topics().getOutputTopic(), record.key(),
                                        record.value())));
                    }
                } finally {
                    if (this.consumer != null) {
                        this.consumer.close();
                    }
                    if (this.producer != null) {
                        this.producer.close();
                    }
                }
            }
        };
    }

    @Override
    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
        return this.getClass().getSimpleName() + "-" + configuration.getTopics().getOutputTopic();
    }

    @Override
    public SerializerDeserializerConfig defaultSerializationConfig() {
        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class, StringDeserializer.class, StringDeserializer.class);
    }

}
