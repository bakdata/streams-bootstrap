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

package com.bakdata.kafka.integration;

import static com.bakdata.kafka.KafkaContainerHelper.DEFAULT_TOPIC_SETTINGS;
import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaContainerHelper;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.test_applications.Mirror;
import com.bakdata.kafka.util.ImprovedAdminClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class RunStreamsAppTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Container
    private final ConfluentKafkaContainer kafkaCluster = newKafkaCluster();

    @Test
    void shouldRunApp() {
        final String input = "input";
        final String output = "output";
        final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
        try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
            admin.getTopicClient().createTopic(output, DEFAULT_TOPIC_SETTINGS, emptyMap());
        }
        try (final KafkaStreamsApplication<?> app = new SimpleKafkaStreamsApplication<>(Mirror::new)) {
            app.setBootstrapServers(this.kafkaCluster.getBootstrapServers());
            app.setKafkaConfig(Map.of(
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
            ));
            app.setInputTopics(List.of(input));
            app.setOutputTopic(output);
            // run in Thread because the application blocks indefinitely
            new Thread(app).start();
            kafkaContainerHelper.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(input, List.of(new KeyValue<>("foo", "bar")));
            assertThat(kafkaContainerHelper.read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from(output, TIMEOUT))
                    .hasSize(1);
        }
    }
}
