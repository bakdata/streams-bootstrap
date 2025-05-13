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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TestApplicationTopologyFactoryTest {

    static final String OUTPUT_TOPIC = "output";
    static final String INPUT_TOPIC = "input";
    @RegisterExtension
    private final TestTopologyExtension<String, String> testTopologyExtension = new TestApplicationTopologyFactory()
            .createTopologyExtension(createApp());

    static KafkaStreamsApplication<SimpleStreamsApp> createApp() {
        return createApp(new SimpleStreamsApp());
    }

    static KafkaStreamsApplication<SimpleStreamsApp> createApp(final SimpleStreamsApp streamsApp) {
        final KafkaStreamsApplication<SimpleStreamsApp> app = newApp(streamsApp);
        app.setInputTopics(List.of(INPUT_TOPIC));
        app.setOutputTopic(OUTPUT_TOPIC);
        return app;
    }

    static KafkaStreamsApplication<SimpleStreamsApp> newApp(final SimpleStreamsApp streamsApp) {
        return new KafkaStreamsApplication<>() {
            @Override
            public SimpleStreamsApp createApp() {
                return streamsApp;
            }
        };
    }

    @Test
    void shouldProcessRecordsWithExtension() {
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
        final TestApplicationTopologyFactory factory = new TestApplicationTopologyFactory();
        try (final TestTopology<String, String> testTopology = factory.createTopology(createApp())) {
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
        final TestApplicationTopologyFactory factory = TestApplicationTopologyFactory.withSchemaRegistry();
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

    @Test
    void shouldCallPrepareRun() {
        final KafkaStreamsApplication<StreamsApp> app = mock();
        when(app.createConfiguredApp()).thenReturn(
                new ConfiguredStreamsApp<>(new SimpleStreamsApp(), StreamsTopicConfig.builder()
                        .inputTopics(List.of(INPUT_TOPIC))
                        .outputTopic(OUTPUT_TOPIC)
                        .build()));
        final TestApplicationTopologyFactory factory = new TestApplicationTopologyFactory();
        try (final TestTopology<String, String> testTopology = factory.createTopology(app)) {
            verify(app).prepareRun();
            testTopology.start();
        }
    }

}
