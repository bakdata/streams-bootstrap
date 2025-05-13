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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class ConsumerGroupVerifierTest extends KafkaTest {

    @Test
    void shouldVerify() {
        final StreamsApp app = new SimpleStreamsApp();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(app, StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .outputTopic("output")
                        .build());
        final RuntimeConfiguration configuration = RuntimeConfiguration.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withRuntimeConfiguration(configuration);
        final KafkaTestClient testClient = new KafkaTestClient(configuration);
        testClient.createTopic("input");
        try (final StreamsRunner runner = executableApp.createRunner()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to("input", List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            new Thread(runner).start();
            awaitActive(executableApp);
            awaitProcessing(executableApp);
            final List<ConsumerRecord<String, String>> records = testClient.read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from("input", Duration.ofSeconds(10L));
            assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        assertThat(rekord.key()).isEqualTo("foo");
                        assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
        awaitClosed(executableApp);
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(executableApp);
        assertThat(verifier.isActive()).isFalse();
    }

}
