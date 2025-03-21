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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ConsumerGroupVerifierTest {

    @Container
    private final KafkaContainer kafkaCluster =
            new KafkaContainer(DockerImageName.parse("apache/kafka") //FIXME native image is flaky
                    .withTag(AppInfoParser.getVersion()));

    private static ConditionFactory await() {
        return Awaitility.await()
                .pollInterval(Duration.ofSeconds(2L))
                .atMost(Duration.ofSeconds(20L));
    }

    @Test
    void shouldVerify() {
        final StreamsApp app = new StreamsApp() {

            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                builder.streamInput().toOutputTopic();
            }

            @Override
            public String getUniqueAppId(final StreamsTopicConfig topics) {
                return "group";
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                return new SerdeConfig(StringSerde.class, StringSerde.class);
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .outputTopic("output")
                        .build(), TestTopologyFactory.createStreamsTestConfig()));
        final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                .bootstrapServers(this.kafkaCluster.getBootstrapServers())
                .build();
        final ExecutableStreamsApp<StreamsApp> executableApp = configuredApp.withEndpoint(endpointConfig);
        final KafkaTestClient testClient = new KafkaTestClient(endpointConfig);
        testClient.createTopic("input");
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(executableApp);
        try (final StreamsRunner runner = executableApp.createRunner()) {
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to("input", List.of(
                            new SimpleProducerRecord<>("foo", "bar")
                    ));
            new Thread(runner).start();
            await().untilAsserted(() -> assertThat(verifier.isActive()).isTrue());
            await().untilAsserted(() -> assertThat(verifier.hasFinishedProcessing()).isTrue());
            testClient.read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .from("input", Duration.ofSeconds(10L));
        }
        await().untilAsserted(() -> assertThat(verifier.isClosed()).isTrue());
    }

}
