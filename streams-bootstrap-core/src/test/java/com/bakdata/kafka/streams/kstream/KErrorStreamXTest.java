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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.ProcessingError;
import com.bakdata.kafka.streams.apps.StringApp;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Test;

class KErrorStreamXTest {

    @Test
    void shouldGetErrorsAndValuesWithKey() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStreamX<String, String, String, String> processed =
                        input.mapCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput("output")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            topology.input().add("foo", "baz");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("success_key")
                    .hasValue("success_value")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGetErrorsAndValuesWithKeyNamed() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStreamX<String, String, String, String> processed =
                        input.mapCapturingErrors(mapper);
                processed.values(Named.as("values")).to("output");
                processed.errors(Named.as("errors"))
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput("output")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            topology.input().add("foo", "baz");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("success_key")
                    .hasValue("success_value")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGetErrorsAndValues() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStreamX<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput("output")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            topology.input().add("foo", "baz");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("success")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGetErrorsAndValuesNamed() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final StreamsBuilderX builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStreamX<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper);
                processed.values(Named.as("value")).to("output");
                processed.errors(Named.as("errors"))
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput("output")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            topology.input().add("foo", "baz");
            topology.streamOutput("output")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("success")
                    .expectNoMoreRecord();
            topology.streamOutput("error")
                    .expectNoMoreRecord();
        }
    }
}
