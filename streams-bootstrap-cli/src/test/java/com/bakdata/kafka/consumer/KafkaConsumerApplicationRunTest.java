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

package com.bakdata.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaConsumerApplicationRunTest extends KafkaTest {

    @Test
    void shouldRunApp() {
        final String input = "input";
        final List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();
        try (final KafkaConsumerApplication<?> app = new SimpleKafkaConsumerApplication<>(() -> new ConsumerApp() {
            @Override
            public DeserializerConfig defaultSerializationConfig() {
                return new DeserializerConfig(StringDeserializer.class, StringDeserializer.class);
            }

            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                return new ConsumerRunnable() {
                    private volatile Consumer<String, String> consumer = null;
                    private volatile boolean running = true;

                    @Override
                    public void close() {
                        this.running = false;
                        this.consumer.wakeup();
                    }

                    @Override
                    public void run() {
                        this.consumer = builder.createConsumer();
                        this.consumer.subscribe(builder.topics().getInputTopics());
                        while (this.running) {
                            final ConsumerRecords<String, String> consumerRecords =
                                    this.consumer.poll(Duration.ofMillis(100L));
                            consumerRecords.forEach(consumedRecords::add);
                        }
                    }
                };
            }

            @Override
            public String getUniqueAppId(final ConsumerAppConfiguration configuration) {
                return "app-id";
            }
        })) {
            app.setBootstrapServers(this.getBootstrapServers());
            final String schemaRegistryUrl = this.getSchemaRegistryUrl();
            app.setSchemaRegistryUrl(schemaRegistryUrl);
            app.setInputTopics(List.of(input));

            new Thread(app).start();
            awaitActive(app.createExecutableApp());

            final KafkaTestClient testClient = this.newTestClient();
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));

            assertThat(consumedRecords).hasSize(1)
                    .anySatisfy(consumerRecord -> {
                        assertThat(consumerRecord.key()).isEqualTo("foo");
                        assertThat(consumerRecord.value()).isEqualTo("bar");
                    });
        }
    }
}
