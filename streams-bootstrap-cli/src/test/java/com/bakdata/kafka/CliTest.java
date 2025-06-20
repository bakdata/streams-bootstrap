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

import static com.bakdata.kafka.KafkaTest.POLL_TIMEOUT;
import static com.bakdata.kafka.KafkaTest.newCluster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

class CliTest {

    private static Thread runApp(final KafkaStreamsApplication<?> app, final String... args) {
        final Thread thread = new Thread(() -> KafkaApplication.startApplication(app, args));
        thread.start();
        return thread;
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        KafkaApplication.startApplication(new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnRunError() {
        KafkaApplication.startApplication(new SimpleKafkaStreamsApplication<>(() -> new StreamsApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId(final StreamsTopicConfig topics) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SerdeConfig defaultSerializationConfig() {
                throw new UnsupportedOperationException();
            }
        }), new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnCleanupError() {
        KafkaApplication.startApplication(new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void clean() {
                throw new RuntimeException();
            }
        }, new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(2)
    void shouldExitWithErrorCodeOnMissingBootstrapServersParameter() {
        KafkaApplication.startApplication(new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnInconsistentAppId() {
        KafkaApplication.startApplication(new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        builder.streamInput().toOutputTopic();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "my-id";
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        return new SerdeConfig(StringSerde.class, StringSerde.class);
                    }
                };
            }
        }, new String[]{
                "--bootstrap-servers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
                "--application-id", "my-other-id"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorInTopology() {
        final String input = "input";
        try (final KafkaContainer kafkaCluster = newCluster();
                final KafkaStreamsApplication<?> app = new SimpleKafkaStreamsApplication<>(() -> new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        builder.streamInput(Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                                .peek((k, v) -> {
                                    throw new RuntimeException();
                                });
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "app";
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                })) {
            kafkaCluster.start();

            final Thread thread = runApp(app,
                    "--bootstrap-server", kafkaCluster.getBootstrapServers(),
                    "--input-topics", input
            );
            new KafkaTestClient(RuntimeConfiguration.create(kafkaCluster.getBootstrapServers()))
                    .send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));
            await("Thread is dead").atMost(Duration.ofSeconds(10L)).until(() -> !thread.isAlive());
        }
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCodeOnShutdown() {
        final String input = "input";
        final String output = "output";
        try (final KafkaContainer kafkaCluster = newCluster();
                final KafkaStreamsApplication<?> app = new SimpleKafkaStreamsApplication<>(() -> new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        builder.streamInput(Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                                .toOutputTopic();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "app";
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        return new SerdeConfig(StringSerde.class, StringSerde.class);
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
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));
            final List<ConsumerRecord<String, String>> keyValues = testClient.read()
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
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
        KafkaApplication.startApplication(new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }, new String[]{
                "--bootstrap-server", "localhost:9092",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    void shouldParseArguments() {
        try (final KafkaStreamsApplication<?> app = new KafkaStreamsApplication<>() {
            @Override
            public StreamsApp createApp() {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final StreamsBuilderX builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerdeConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }) {
            KafkaApplication.startApplicationWithoutExit(app, new String[]{
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
