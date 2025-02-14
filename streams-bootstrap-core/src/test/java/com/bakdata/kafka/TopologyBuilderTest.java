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

import static com.bakdata.kafka.ImprovedKStreamTest.startApp;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

class TopologyBuilderTest {

    @Test
    void shouldReadFromInput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.streamInput();
                input.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldReadFromInputUsingConsumed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.streamInput(
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .build());
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
    void shouldReadFromLabeledInput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.streamInput("label");
                input.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledInputTopics(Map.of("label", List.of("input")))
                        .build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldReadFromLabeledInputUsingConsumed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.streamInput("label",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledInputTopics(Map.of("label", List.of("input")))
                        .build());
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
    void shouldReadFromPatternInput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.streamInputPattern();
                input.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .inputPattern(Pattern.compile("input\\d+"))
                        .build());
        topology.input("input1").add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldReadFromPatternInputUsingConsumed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.streamInputPattern(
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .inputPattern(Pattern.compile("input\\d+"))
                        .build());
        topology.input("input1")
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
    void shouldReadFromLabeledPatternInput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.streamInputPattern("label");
                input.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledInputPatterns(Map.of("label", Pattern.compile("input\\d+")))
                        .build());
        topology.input("input1").add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldReadFromLabeledPatternInputUsingConsumed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.streamInputPattern("label",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledInputPatterns(Map.of("label", Pattern.compile("input\\d+")))
                        .build());
        topology.input("input1")
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

}
