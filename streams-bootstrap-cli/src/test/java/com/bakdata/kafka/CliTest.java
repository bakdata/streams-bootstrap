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

package com.bakdata.kafka;

import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.Test;

class CliTest {

    private static void runApp(final KafkaStreamsApplication app, final String... args) {
        new Thread(() -> KafkaApplication.startApplication(app, args)).start();
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        KafkaApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--brokers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnRunError() {
        KafkaApplication.startApplication(new SimpleKafkaStreamsApplication(() -> new StreamsApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId(final StreamsTopicConfig topics) {
                throw new UnsupportedOperationException();
            }
        }), new String[]{
                "--brokers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCodeOnCleanupError() {
        KafkaApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void clean() {
                throw new RuntimeException();
            }
        }, new String[]{
                "--brokers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(2)
    void shouldExitWithErrorCodeOnMissingBrokerParameter() {
        KafkaApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(2)
    void shouldExitWithErrorCodeOnInconsistentAppId() {
        KafkaApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        builder.streamInput().to(builder.getTopics().getOutputTopic());
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "my-id";
                    }
                };
            }
        }, new String[]{
                "--brokers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
                "--application-id", "my-other-id"
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorInTopology() throws InterruptedException {
        final String input = "input";
        try (final EmbeddedKafkaCluster kafkaCluster = newKafkaCluster();
                final KafkaStreamsApplication app = new SimpleKafkaStreamsApplication(() -> new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        builder.streamInput(Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                                .peek((k, v) -> {
                                    throw new RuntimeException();
                                });
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "app";
                    }
                })) {
            kafkaCluster.start();
            kafkaCluster.createTopic(TopicConfig.withName(input).build());

            runApp(app,
                    "--brokers", kafkaCluster.getBrokerList(),
                    "--schema-registry-url", "http://localhost:8081",
                    "--input-topics", input
            );
            kafkaCluster.send(SendKeyValues.to(input, List.of(new KeyValue<>("foo", "bar"))));
            delay(10, TimeUnit.SECONDS);
        }
    }

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCodeOnShutdown() throws InterruptedException {
        final String input = "input";
        final String output = "output";
        try (final EmbeddedKafkaCluster kafkaCluster = newKafkaCluster();
                final KafkaStreamsApplication app = new SimpleKafkaStreamsApplication(() -> new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        builder.streamInput(Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                                .to(builder.getTopics().getOutputTopic());
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        return "app";
                    }
                })) {
            kafkaCluster.start();
            kafkaCluster.createTopic(TopicConfig.withName(input).build());
            kafkaCluster.createTopic(TopicConfig.withName(output).build());

            runApp(app,
                    "--brokers", kafkaCluster.getBrokerList(),
                    "--schema-registry-url", "http://localhost:8081",
                    "--input-topics", input,
                    "--output-topic", output
            );
            kafkaCluster.send(SendKeyValues.to(input, List.of(new KeyValue<>("foo", "bar"))));
            delay(10, TimeUnit.SECONDS);
            final List<KeyValue<String, String>> keyValues = kafkaCluster.read(ReadKeyValues.from(output));
            assertThat(keyValues)
                    .hasSize(1)
                    .anySatisfy(kv -> {
                        assertThat(kv.getKey()).isEqualTo("foo");
                        assertThat(kv.getValue()).isEqualTo("bar");
                    });
        }
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorOnCleanupError() {
        KafkaApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }, new String[]{
                "--brokers", "localhost:9092",
                "--schema-registry-url", "http://localhost:8081",
                "--input-topics", "input",
                "--output-topic", "output",
                "clean",
        });
    }

    @Test
    void shouldParseArguments() {
        try (final KafkaStreamsApplication app = new KafkaStreamsApplication() {
            @Override
            public StreamsApp createApp(final boolean cleanUp) {
                return new StreamsApp() {
                    @Override
                    public void buildTopology(final TopologyBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public String getUniqueAppId(final StreamsTopicConfig topics) {
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
                    "--brokers", "brokers",
                    "--schema-registry-url", "schema-registry",
                    "--input-topics", "input1,input2",
                    "--extra-input-topics", "role1=input3,role2=input4;input5",
                    "--input-pattern", ".*",
                    "--extra-input-patterns", "role1=.+,role2=\\d+",
                    "--output-topic", "output1",
                    "--extra-output-topics", "role1=output2,role2=output3",
                    "--kafka-config", "foo=1,bar=2",
            });
            assertThat(app.getInputTopics()).containsExactly("input1", "input2");
            assertThat(app.getExtraInputTopics())
                    .hasSize(2)
                    .containsEntry("role1", List.of("input3"))
                    .containsEntry("role2", List.of("input4", "input5"));
            assertThat(app.getInputPattern())
                    .satisfies(pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".*").pattern()));
            assertThat(app.getExtraInputPatterns())
                    .hasSize(2)
                    .hasEntrySatisfying("role1",
                            pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".+").pattern()))
                    .hasEntrySatisfying("role2",
                            pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile("\\d+").pattern()));
            assertThat(app.getOutputTopic()).isEqualTo("output1");
            assertThat(app.getExtraOutputTopics())
                    .hasSize(2)
                    .containsEntry("role1", "output2")
                    .containsEntry("role2", "output3");
            assertThat(app.getKafkaConfig())
                    .hasSize(2)
                    .containsEntry("foo", "1")
                    .containsEntry("bar", "2");
        }
    }
}
