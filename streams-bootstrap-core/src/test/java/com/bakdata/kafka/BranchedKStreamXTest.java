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

import static com.bakdata.kafka.KStreamXTest.startApp;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.Map;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

class BranchedKStreamXTest {

    @Test
    void shouldBranch() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStream<String, String>> branches = input.split()
                        .branch((k, v) -> "foo".equals(k))
                        .noDefaultBranch();
                branches.values().iterator().next().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldBranchBranched() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .branch((k, v) -> "foo".equals(k), Branched.withConsumer(branch -> branch.to("output")));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldBranchBranchedX() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .branch((k, v) -> "foo".equals(k), BranchedX.withConsumer(KStreamX::toOutputTopic));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .outputTopic("output")
                        .build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldUseDefaultBranch() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final Map<String, KStream<String, String>> branches = input.split()
                        .defaultBranch();
                branches.values().iterator().next().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldUseDefaultBranched() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .defaultBranch(Branched.withConsumer(branch -> branch.to("output")));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldUseDefaultBranchedX() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.split()
                        .defaultBranch(BranchedX.withConsumer(KStreamX::toOutputTopic));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .outputTopic("output")
                        .build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }
}
