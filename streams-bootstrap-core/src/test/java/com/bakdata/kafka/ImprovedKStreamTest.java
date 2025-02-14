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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import io.vavr.collection.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ImprovedKStreamTest {

    static <K, V> TestTopology<K, V> startApp(final StreamsApp app, final StreamsTopicConfig topicConfig) {
        final ConfiguredStreamsApp<StreamsApp> configuredStreamsApp =
                new ConfiguredStreamsApp<>(app, new AppConfiguration<>(topicConfig));
        final TestTopology<K, V> topology =
                TestTopologyFactory.withoutSchemaRegistry().createTopology(configuredStreamsApp);
        topology.start();
        return topology;
    }

    @Test
    void shouldWriteToOutput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toOutputTopic();
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .outputTopic("output")
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .outputTopic("output")
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
    void shouldWriteToLabeledOutput() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toOutputTopic("label");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledOutputTopics(Map.of("label", "output"))
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .labeledOutputTopics(Map.of("label", "output"))
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
    void shouldWriteToError() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                input.toErrorTopic();
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .errorTopic("error")
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder()
                        .errorTopic("error")
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
    void shouldMapCapturingErrors() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldMapValuesCapturingErrors() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldMapValuesWithKeyCapturingErrors() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn("success").when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldFlatMapCapturingErrors() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of(KeyValue.pair("success_key", "success_value"))).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldFlatMapValuesCapturingErrors() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn(List.of("success")).when(mapper).apply("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldFlatMapValuesWithKeyCapturingErrors() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of("success")).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldProcessCapturingErrors() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new Processor<>() {
            private ProcessorContext<String, String> context = null;

            @Override
            public void init(final ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.context.forward(inputRecord.withKey("success_key").withValue("success_value"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processCapturingErrors(processor);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldProcessValuesCapturingErrors() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context = null;

            @Override
            public void init(final FixedKeyProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.context.forward(inputRecord.withValue("success"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processValuesCapturingErrors(processor);
                processed.values().to("output");
                processed.errors()
                        .mapValues(ProcessingError::getValue)
                        .to("error");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldRepartition() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKStream<Long, Long> repartitioned = input.repartition(
                        ConfiguredRepartitioned.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                repartitioned.to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
    void shouldConvertToTable() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final ImprovedKStream<Long, Long> input = builder.stream("input",
                        ConfiguredConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final ImprovedKTable<Long, Long> table = input.toTable(
                        ConfiguredMaterialized.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                table.toStream().to("output", ConfiguredProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
}
