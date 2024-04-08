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

package com.bakdata.kafka.integration;

import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.test_applications.Mirror;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RunStreamsAppTest {
    private static final int TIMEOUT_SECONDS = 10;
    private final EmbeddedKafkaCluster kafkaCluster = newKafkaCluster();

    @BeforeEach
    void setup() {
        this.kafkaCluster.start();
    }

    @AfterEach
    void tearDown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldRunApp() throws InterruptedException {
        final String input = "input";
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.withName(input).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
        try (final KafkaStreamsApplication app = new SimpleKafkaStreamsApplication(Mirror::new)) {
            app.setBrokers(this.kafkaCluster.getBrokerList());
            app.setKafkaConfig(Map.of(
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
            ));
            app.setInputTopics(List.of(input));
            app.setOutputTopic(output);
            // run in Thread because the application blocks indefinitely
            new Thread(app).start();
            final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
                    SendKeyValuesTransactional.inTransaction(input, List.of(new KeyValue<>("foo", "bar")))
                            .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                            .build();
            this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, String.class)
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .build()))
                    .hasSize(1);
        }
    }
}
