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

package com.bakdata.kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.TestApplicationRunner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class KafkaProducerApplicationRunTest extends KafkaTest {

    @Test
    void shouldRunApp() {
        final String output = "output";
        try (final KafkaProducerApplication<?> app = new SimpleKafkaProducerApplication<>(() -> new ProducerApp() {
            @Override
            public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                return () -> {
                    try (final Producer<String, String> producer = builder.createProducer()) {
                        final String testRecord = "bar";
                        producer.send(new ProducerRecord<>(builder.getTopics().getOutputTopic(), "foo", testRecord));
                    }
                };
            }

            @Override
            public SerializerConfig defaultSerializationConfig() {
                return new SerializerConfig(StringSerializer.class, StringSerializer.class);
            }
        })) {
            final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers());
            app.setOutputTopic(output);
            final KafkaTestClient testClient = runner.newTestClient();
            testClient.createTopic(output);
            runner.run(app);
            assertThat(testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new StringDeserializer())
                    .from(output, POLL_TIMEOUT))
                    .hasSize(1)
                    .anySatisfy(kv -> {
                        assertThat(kv.key()).isEqualTo("foo");
                        assertThat(kv.value()).isEqualTo("bar");
                    });
            runner.clean(app);
            assertThat(testClient.existsTopic(app.getOutputTopic()))
                    .as("Output topic is deleted")
                    .isFalse();
        }
    }
}
