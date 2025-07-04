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

import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerGroupVerifierTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldVerify() {
        final StreamsApp app = new SimpleStreamsApp();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .outputTopic("output")
                        .build()));
        final RuntimeConfiguration configuration = RuntimeConfiguration.create(this.getBootstrapServers())
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
        final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withRuntimeConfiguration(configuration);
        final KafkaTestClient testClient = new KafkaTestClient(configuration);
        testClient.createTopic("input");
        try (final StreamsRunner runner = executableApp.createRunner()) {
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to("input", List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            runAsync(runner);
            awaitProcessing(executableApp);
            final List<ConsumerRecord<String, String>> records = testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new StringDeserializer())
                    .from("output", POLL_TIMEOUT);
            this.softly.assertThat(records)
                    .hasSize(1)
                    .anySatisfy(rekord -> {
                        this.softly.assertThat(rekord.key()).isEqualTo("foo");
                        this.softly.assertThat(rekord.value()).isEqualTo("bar");
                    });
        }
        awaitClosed(executableApp);
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(executableApp);
        this.softly.assertThat(verifier.isActive()).isFalse();
    }

}
