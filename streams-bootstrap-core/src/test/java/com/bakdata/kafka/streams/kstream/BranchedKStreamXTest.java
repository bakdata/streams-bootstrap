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

package com.bakdata.kafka.streams.kstream;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import com.bakdata.kafka.streams.apps.StringApp;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

class BranchedKStreamXTest {

    @Test
    void shouldBranch() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStream<String, String>> branches = input.split()
                        .branch((k, v) -> "foo".equals(k))
                        .noDefaultBranch();
                branches.values().iterator().next().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldBranchBranched() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .branch((k, v) -> "foo".equals(k), Branched.withConsumer(branch -> branch.to("output")));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldBranchBranchedX() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .branch((k, v) -> "foo".equals(k), BranchedX.withConsumer(KStreamX::toOutputTopic))
                        .defaultBranchX(Branched.withConsumer(s -> s.to("default_output")));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "qux");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            topology.streamOutput("default_output")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseDefaultBranch() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStream<String, String>> branches = input.split()
                        .defaultBranch();
                branches.values().iterator().next().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseDefaultBranchX() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStreamX<String, String>> branches = input.split()
                        .defaultBranchX();
                branches.values().iterator().next().toOutputTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseDefaultBranchBranched() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .defaultBranch(Branched.withConsumer(branch -> branch.to("output")));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseDefaultBranchXBranched() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStreamX<String, String>> branches = input.split()
                        .defaultBranchX(Branched.withFunction(Function.identity()));
                branches.values().iterator().next().toOutputTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseDefaultBranchBranchedX() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStreamX<String, String>> branches = input.split()
                        .defaultBranch(BranchedX.withFunction(Function.identity()));
                branches.values().iterator().next().toOutputTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldUseNoDefaultBranchX() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStreamX<String, String>> branches = input.split()
                        .branch((k, v) -> "foo".equals(k))
                        .noDefaultBranchX();
                branches.values().iterator().next().toOutputTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }
}
