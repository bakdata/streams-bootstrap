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

import static com.bakdata.kafka.KafkaTest.newCluster;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.consumer.apps.StringConsumer;
import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class KafkaConsumerApplicationCliTest {

    private static CompletableFuture<Void> runApp(final KafkaConsumerApplication<?> app, final String... args) {
        return runAsync(() -> app.startApplication(args));
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueGroupId(final ConsumerAppConfiguration configuration) {
                        return "group-id";
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
                "--group-id", "group-id"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnRunError() {
        new SimpleKafkaConsumerApplication<>(() -> new ConsumerApp() {
            @Override
            public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DeserializerConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }
        }).startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnCleanupError() {
        new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }.startApplication(new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "clean",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(2)
    void shouldExitWithErrorCodeOnMissingBootstrapServersParameter() {
        new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }.startApplication(new String[]{
                "--input-topics", "input"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnInconsistentAppId() {
        new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }.startApplication(new String[]{
                "--bootstrap-servers", "localhost:9092",
                "--input-topics", "input",
                "--group-id", "my-other-id"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorInRecordProcessor() {
        final String input = "input";
        try (final KafkaContainer kafkaCluster = newCluster();
                final KafkaConsumerApplication<?> app = new SimpleKafkaConsumerApplication<>(() -> new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        final Consumer<String, String> consumer = builder.createConsumer();
                        builder.subscribeToAllTopics(consumer);
                        return builder.createDefaultConsumerRunnable(consumer,
                                records -> {throw new RuntimeException();});
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
                        return new DeserializerConfig(StringDeserializer.class, StringDeserializer.class);
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
                final StringConsumer stringConsumer = new StringConsumer();
                final KafkaConsumerApplication<?> app = new SimpleKafkaConsumerApplication<>(() -> stringConsumer)) {
            kafkaCluster.start();
            final KafkaTestClient testClient =
                    new KafkaTestClient(RuntimeConfiguration.create(kafkaCluster.getBootstrapServers()));
            testClient.createTopic(output);

            runApp(app,
                    "--bootstrap-server", kafkaCluster.getBootstrapServers(),
                    "--input-topics", input
            );
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));
            await().atMost(Duration.ofSeconds(1L))
                    .untilAsserted(() -> assertThat(stringConsumer.getConsumedRecords())
                            .hasSize(1)
                            .anySatisfy(kv -> {
                                assertThat(kv.key()).isEqualTo("foo");
                                assertThat(kv.value()).isEqualTo("bar");
                            }));
        }
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorOnCleanupError() {
        new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {
                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
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
                "clean",
        });
    }

    @Test
    void shouldParseArguments() {
        try (final KafkaConsumerApplication<?> app = new KafkaConsumerApplication<>() {
            @Override
            public ConsumerApp createApp() {
                return new ConsumerApp() {

                    @Override
                    public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public DeserializerConfig defaultSerializationConfig() {
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
                    "--poll-timeout", "PT1S"
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
            assertThat(app.getPollTimeout()).isEqualTo(Duration.ofSeconds(1));
        }
    }
}
