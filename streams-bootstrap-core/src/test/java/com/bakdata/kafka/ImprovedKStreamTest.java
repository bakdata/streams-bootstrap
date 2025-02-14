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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.junit.jupiter.api.Test;

class ImprovedKStreamTest {

    @Test
    void shouldWriteToOutput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toOutputTopic();
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .outputTopic("output")
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldWriteToOutputUsingProduced() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toOutputTopic(ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .outputTopic("output")
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .add(1L, 2L);
        topology.streamOutput()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(2L)
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldWriteToLabeledOutput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toOutputTopic("label");
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .labeledOutputTopics(Map.of("label", "output"))
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldWriteToLabeledOutputUsingProduced() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toOutputTopic("label", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .labeledOutputTopics(Map.of("label", "output"))
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .add(1L, 2L);
        topology.streamOutput()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(2L)
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldWriteToError() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toErrorTopic();
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .errorTopic("error")
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldWriteToErrorUsingProduced() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toErrorTopic(ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(StreamsTopicConfig.builder()
                        .errorTopic("error")
                        .build()));
        final TestTopology<String, String> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        topology.input()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .add(1L, 2L);
        topology.streamOutput()
                .withKeySerde(Serdes.Long())
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKey(1L)
                .hasValue(2L)
                .expectNoMoreRecord();
        topology.stop();
    }

    private abstract static class SimpleApp implements StreamsApp {

        @Override
        public String getUniqueAppId(final StreamsTopicConfig topics) {
            return "my-app";
        }

        @Override
        public SerdeConfig defaultSerializationConfig() {
            return new SerdeConfig(StringSerde.class, StringSerde.class);
        }
    }
}
