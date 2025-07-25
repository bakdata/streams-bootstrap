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

package com.bakdata.kafka;

import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class KafkaTestClientTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldSendAndReadWithKeyAndValueSerde() {
        final KafkaTestClient testClient = this.newTestClient();
        final String topic = "topic";
        testClient.createTopic(topic);
        testClient.send()
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(new StringSerializer())
                .to(topic, List.of(new SimpleProducerRecord<>("key", "value")));
        final List<ConsumerRecord<String, String>> records = testClient.read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .from(topic, POLL_TIMEOUT);
        this.softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(rekord -> {
                    this.softly.assertThat(rekord.key()).isEqualTo("key");
                    this.softly.assertThat(rekord.value()).isEqualTo("value");
                });
    }

    @Test
    void shouldSendAndReadWithKeyAndValueSerdeClass() {
        final KafkaTestClient testClient = this.newTestClient();
        final String topic = "topic";
        testClient.createTopic(topic);
        testClient.send()
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .to(topic, List.of(new SimpleProducerRecord<>("key", "value")));
        final List<ConsumerRecord<String, String>> records = testClient.<String, String>read()
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .from(topic, POLL_TIMEOUT);
        this.softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(rekord -> {
                    this.softly.assertThat(rekord.key()).isEqualTo("key");
                    this.softly.assertThat(rekord.value()).isEqualTo("value");
                });
    }

    @Test
    void shouldSendAndRead() {
        final KafkaTestClient testClient = this.newTestClient();
        final String topic = "topic";
        testClient.createTopic(topic);
        testClient.send()
                .withSerializers(new StringSerializer(), new StringSerializer())
                .to(topic, List.of(new SimpleProducerRecord<>("key", "value")));
        final List<ConsumerRecord<String, String>> records = testClient.read()
                .withDeserializers(new StringDeserializer(), new StringDeserializer())
                .from(topic, POLL_TIMEOUT);
        this.softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(rekord -> {
                    this.softly.assertThat(rekord.key()).isEqualTo("key");
                    this.softly.assertThat(rekord.value()).isEqualTo("value");
                });
    }

    @Test
    void shouldCreateTopic() {
        final KafkaTestClient testClient = this.newTestClient();
        final String topic = "topic";
        this.softly.assertThat(testClient.existsTopic(topic)).isFalse();
        testClient.createTopic(topic);
        this.softly.assertThat(testClient.existsTopic(topic)).isTrue();
    }
}
