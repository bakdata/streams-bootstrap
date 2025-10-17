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

package com.bakdata.kafka.consumerproducer;

import static com.bakdata.kafka.KafkaTest.POLL_TIMEOUT;
import static com.bakdata.kafka.KafkaTest.newCluster;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class KafkaConsumerProducerApplicationCliTest {

    private static CompletableFuture<Void> runApp(final KafkaConsumerProducerApplication<?> app, final String... args) {
        return runAsync(() -> app.startApplication(args));
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }.startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnRunError() {
        new SimpleKafkaConsumerProducerApplication<>(() -> new ConsumerProducerApp() {
            @Override
            public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SerializerDeserializerConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }
        }).startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnCleanupError() {
        new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void clean() {
                throw new RuntimeException();
            }
        }.startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(2)
    void shouldExitWithErrorCodeOnMissingBootstrapServersParameter() {
        new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }.startApplication(new String[]{
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnInconsistentAppId() {
        new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        return () -> {};
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        return "my-id";
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class,
                                StringDeserializer.class, StringDeserializer.class);
                    }
                };
            }
        }.startApplication(new String[]{
                "--bootstrap-servers", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
                "--application-id", "my-other-id"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorInBuildRunnable() {
        final String input = "input";
        try (final KafkaContainer kafkaCluster = newCluster();
                final KafkaConsumerProducerApplication<?> app = new SimpleKafkaConsumerProducerApplication<>(() -> new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        return () -> {
                            throw new RuntimeException("Error building runnable");
                        };
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        return "app";
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class,
                                StringDeserializer.class, StringDeserializer.class);
                    }
                })) {
            kafkaCluster.start();

            final CompletableFuture<Void> future = runApp(app,
                    "--bootstrap-server", kafkaCluster.getBootstrapServers(),
                    "--input-topics", input
            );
            new KafkaTestClient(RuntimeConfiguration.create(kafkaCluster.getBootstrapServers()))
                    .send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));
            await("Application has closed").atMost(Duration.ofSeconds(10L)).until(future::isDone);
        }
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCodeOnShutdown() {
        final String input = "input";
        final String output = "output";
        try (final KafkaContainer kafkaCluster = newCluster();
                final KafkaConsumerProducerApplication<?> app = new SimpleKafkaConsumerProducerApplication<>(() -> new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        return () -> {
                            try (final Producer<String, String> producer = builder.producerBuilder().createProducer()) {
                                final ProducerRecord<String, String> record = new ProducerRecord<>(output, "foo", "bar");
                                producer.send(record);
                            }
                        };
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        return "app";
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        return new SerializerDeserializerConfig(StringSerializer.class, StringSerializer.class,
                                StringDeserializer.class, StringDeserializer.class);
                    }
                })) {
            kafkaCluster.start();
            final KafkaTestClient testClient =
                    new KafkaTestClient(RuntimeConfiguration.create(kafkaCluster.getBootstrapServers()));
            testClient.createTopic(output);

            runApp(app,
                    "--bootstrap-server", kafkaCluster.getBootstrapServers(),
                    "--input-topics", input,
                    "--output-topic", output
            );
            final List<ConsumerRecord<String, String>> keyValues = testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new StringDeserializer())
                    .from(output, POLL_TIMEOUT);
            assertThat(keyValues)
                    .hasSize(1)
                    .anySatisfy(kv -> {
                        assertThat(kv.key()).isEqualTo("foo");
                        assertThat(kv.value()).isEqualTo("bar");
                    });
        }
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorOnCleanupError() {
        new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }.startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    void shouldParseArguments() {
        try (final KafkaConsumerProducerApplication<?> app = new KafkaConsumerProducerApplication<>() {
            @Override
            public ConsumerProducerApp createApp() {
                return new ConsumerProducerApp() {
                    @Override
                    public ConsumerProducerRunnable buildRunnable(final ConsumerProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerDeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }) {
            app.startApplicationWithoutExit(new String[]{
                    "--bootstrap-server", "bootstrap-servers",
                    "--schema-registry-url", "schema-registry",
                    "--input-topics", "input1,input2",
                    "--labeled-input-topics", "label1=input3,label2=input4;input5",
                    "--input-pattern", ".*",
                    "--labeled-input-patterns", "label1=.+,label2=\\d+",
                    "--output-topic", "output1",
                    "--labeled-output-topics", "label1=output2,label2=output3",
                    "--kafka-config", "foo=1,bar=2",
            });
            assertThat(app.getBootstrapServers()).isEqualTo("bootstrap-servers");
            assertThat(app.getSchemaRegistryUrl()).isEqualTo("schema-registry");
            assertThat(app.getInputTopics()).containsExactly("input1", "input2");
            assertThat(app.getLabeledInputTopics())
                    .hasSize(2)
                    .containsEntry("label1", List.of("input3"))
                    .containsEntry("label2", List.of("input4", "input5"));
            assertThat(app.getInputPattern())
                    .satisfies(pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".*").pattern()));
            assertThat(app.getLabeledInputPatterns())
                    .hasSize(2)
                    .hasEntrySatisfying("label1",
                            pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".+").pattern()))
                    .hasEntrySatisfying("label2",
                            pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile("\\d+").pattern()));
            assertThat(app.getOutputTopic()).isEqualTo("output1");
            assertThat(app.getLabeledOutputTopics())
                    .hasSize(2)
                    .containsEntry("label1", "output2")
                    .containsEntry("label2", "output3");
            assertThat(app.getKafkaConfig())
                    .hasSize(2)
                    .containsEntry("foo", "1")
                    .containsEntry("bar", "2");
        }
    }
}
