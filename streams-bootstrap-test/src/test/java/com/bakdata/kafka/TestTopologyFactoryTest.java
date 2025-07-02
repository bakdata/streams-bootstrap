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
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class TestTopologyFactoryTest {

    @RegisterExtension
    private final TestTopologyExtension<String, String> testTopologyExtension = new TestTopologyFactory()
            .createTopologyExtension(createApp());

    private static ConfiguredStreamsApp<SimpleStreamsApp> createApp() {
        final SimpleStreamsApp app = new SimpleStreamsApp();
        return createApp(app);
    }

    private static ConfiguredStreamsApp<SimpleStreamsApp> createApp(final SimpleStreamsApp app) {
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    @Test
    void shouldProcessRecordsUsingExtension() {
        this.testTopologyExtension.input()
                .add("foo", "bar");
        this.testTopologyExtension.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
    }

    @Test
    void shouldProcessRecords() {
        try (final TestTopology<String, String> testTopology = new TestTopologyFactory().createTopology(createApp())) {
            testTopology.start();
            testTopology.input()
                    .add("foo", "bar");
            testTopology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcessRecordsUsingSchemaRegistry() {
        final TestTopologyFactory factory = TestTopologyFactory.withSchemaRegistry();
        final SimpleStreamsApp app = new SimpleStreamsApp() {
            @Override
            public SerdeConfig defaultSerializationConfig() {
                return super.defaultSerializationConfig()
                        .withValueSerde(SpecificAvroSerde.class);
            }
        };
        try (final TestTopology<String, TestRecord> testTopology = factory.createTopology(createApp(app))) {
            testTopology.start();
            final TestRecord value = TestRecord.newBuilder()
                    .setContent("content")
                    .build();
            testTopology.input()
                    .add("foo", value);
            testTopology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(value)
                    .expectNoMoreRecord();
        }
    }

}
