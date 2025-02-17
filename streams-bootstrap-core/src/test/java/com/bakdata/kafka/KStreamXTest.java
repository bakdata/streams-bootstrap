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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(SoftAssertionsExtension.class)
class KStreamXTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldWriteToOutput() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.toOutputTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldWriteToOutputUsingProduced() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                input.toOutputTopic(ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .outputTopic("output")
                .build())) {
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
        }
    }

    @Test
    void shouldWriteToLabeledOutput() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.toOutputTopic("label");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .labeledOutputTopics(Map.of("label", "output"))
                .build())) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldWriteToLabeledOutputUsingProduced() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                input.toOutputTopic("label", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .labeledOutputTopics(Map.of("label", "output"))
                .build())) {
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
        }
    }

    @Test
    void shouldWriteToError() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.toErrorTopic();
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .errorTopic("error")
                .build())) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldWriteToErrorUsingProduced() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                input.toErrorTopic(ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp(StreamsTopicConfig.builder()
                .errorTopic("error")
                .build())) {
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
        }
    }

    @Test
    void shouldMap() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(new KeyValue<>("baz", "qux"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.map(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapNamed() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(new KeyValue<>("baz", "qux"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.map(mapper, Named.as("map")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValues() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesNamed() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper, Named.as("map")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKey() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapValuesWithKeyNamed() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper, Named.as("map")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMap() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of(new KeyValue<>("baz", "qux")));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMap(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMapNamed() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of(new KeyValue<>("baz", "qux")));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMap(mapper, Named.as("flatMap")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMapValues() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        when(mapper.apply("bar")).thenReturn(List.of("baz"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMapValuesNamed() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        when(mapper.apply("bar")).thenReturn(List.of("baz"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper, Named.as("flatMap")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMapValuesWithKey() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of("baz"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFlatMapValuesWithKeyNamed() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of("baz"));
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper, Named.as("flatMap")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcess() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    this.forward(inputRecord.withKey("baz").withValue("qux"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.process(processor);
                processed.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcessNamed() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    this.forward(inputRecord.withKey("baz").withValue("qux"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.process(processor, Named.as("process"));
                processed.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcessUsingStore() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                final KeyValueStore<String, String> store = this.getStateStore("my-store");
                store.put(inputRecord.key(), inputRecord.value());
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KStreamX<String, String> input = builder.stream("input");
                input.process(processor, "my-store");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldProcessNamedUsingStore() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                final KeyValueStore<String, String> store = this.getStateStore("my-store");
                store.put(inputRecord.key(), inputRecord.value());
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KStreamX<String, String> input = builder.stream("input");
                input.process(processor, Named.as("process"), "my-store");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldProcessValues() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    this.forward(inputRecord.withValue("baz"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.processValues(processor);
                processed.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcessValuesNamed() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    this.forward(inputRecord.withValue("baz"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.processValues(processor, Named.as("process"));
                processed.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldProcessValuesUsingStore() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                final KeyValueStore<String, String> store = this.getStateStore("my-store");
                store.put(inputRecord.key(), inputRecord.value());
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KStreamX<String, String> input = builder.stream("input");
                input.processValues(processor, "my-store");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldProcessValuesNamedUsingStore() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                final KeyValueStore<String, String> store = this.getStateStore("my-store");
                store.put(inputRecord.key(), inputRecord.value());
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final StoreBuilder<KeyValueStore<String, String>> store = builder.stores()
                        .keyValueStoreBuilder(Stores.inMemoryKeyValueStore("my-store"), Preconfigured.defaultSerde(),
                                Preconfigured.defaultSerde());
                builder.addStateStore(store);
                final KStreamX<String, String> input = builder.stream("input");
                input.processValues(processor, Named.as("process"), "my-store");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input().add("foo", "bar");
            final KeyValueStore<String, String> store =
                    topology.getTestDriver().getKeyValueStore("my-store");
            this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        }
    }

    @Test
    void shouldFilter() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("foo", "baz")).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filter(predicate).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("foo", "baz")).thenReturn(false);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filter(predicate, Named.as("filter")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNot() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("foo", "baz")).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filterNot(predicate).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldFilterNotNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("foo", "baz")).thenReturn(true);
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filterNot(predicate, Named.as("filterNot")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldSelectKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.selectKey((k, v) -> v).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldSelectKeyNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.selectKey((k, v) -> v, Named.as("select")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue("bar")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMapCapturingErrors() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
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
    void shouldMapCapturingErrorsNamed() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapCapturingErrors(mapper, Named.as("map"));
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
    void shouldMapValuesCapturingErrors() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
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
    void shouldMapValuesCapturingErrorsNamed() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper, Named.as("map"));
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
    void shouldMapValuesWithKeyCapturingErrors() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn("success").when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
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
    void shouldMapValuesWithKeyCapturingErrorsNamed() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn("success").when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.mapValuesCapturingErrors(mapper, Named.as("map"));
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
    void shouldFlatMapCapturingErrors() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of(KeyValue.pair("success_key", "success_value"))).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapCapturingErrors(mapper);
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
    void shouldFlatMapCapturingErrorsNamed() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of(KeyValue.pair("success_key", "success_value"))).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapCapturingErrors(mapper, Named.as("flatMap"));
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
    void shouldFlatMapValuesCapturingErrors() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn(List.of("success")).when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper);
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
    void shouldFlatMapValuesCapturingErrorsNamed() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn(List.of("success")).when(mapper).apply("baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper, Named.as("flatMap"));
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
    void shouldFlatMapValuesWithKeyCapturingErrors() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of("success")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper);
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
    void shouldFlatMapValuesWithKeyCapturingErrorsNamed() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of("success")).when(mapper).apply("foo", "baz");
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.flatMapValuesCapturingErrors(mapper, Named.as("flatMap"));
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
    void shouldProcessCapturingErrors() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.forward(inputRecord.withKey("success_key").withValue("success_value"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processCapturingErrors(processor);
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
    void shouldProcessCapturingErrorsNamed() {
        final ProcessorSupplier<String, String, String, String> processor = () -> new SimpleProcessor<>() {

            @Override
            public void process(final Record<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.forward(inputRecord.withKey("success_key").withValue("success_value"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processCapturingErrors(processor, Named.as("process"));
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
    void shouldProcessValuesCapturingErrors() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.forward(inputRecord.withValue("success"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processValuesCapturingErrors(processor);
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
    void shouldProcessValuesCapturingErrorsNamed() {
        final FixedKeyProcessorSupplier<String, String, String> processor = () -> new SimpleFixedKeyProcessor<>() {

            @Override
            public void process(final FixedKeyRecord<String, String> inputRecord) {
                if ("foo".equals(inputRecord.key()) && "bar".equals(inputRecord.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if ("foo".equals(inputRecord.key()) && "baz".equals(inputRecord.value())) {
                    this.forward(inputRecord.withValue("success"));
                    return;
                }
                throw new UnsupportedOperationException();
            }
        };
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KErrorStream<String, String, String, String> processed =
                        input.processValuesCapturingErrors(processor, Named.as("process"));
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
    void shouldRepartition() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition();
                repartitioned.to("output");
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
    void shouldRepartitionUsingRepartitioned() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> repartitioned =
                        input.repartition(RepartitionedX.with(Serdes.Long(), Serdes.Long()));
                repartitioned.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
        }
    }

    @Test
    void shouldConvertToTable() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KTableX<String, String> table = input.toTable();
                table.toStream().to("output");
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
    void shouldConvertToTableNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KTableX<String, String> table = input.toTable(Named.as("toTable"));
                table.toStream().to("output");
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
    void shouldConvertToTableUsingMaterialized() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> table =
                        input.toTable(MaterializedX.with(Serdes.Long(), Serdes.Long()));
                table.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
        }
    }

    @Test
    void shouldConvertToTableNamedUsingMaterialized() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> table = input.toTable(Named.as("toTable"),
                        MaterializedX.with(Serdes.Long(), Serdes.Long()));
                table.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
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
        }
    }

    @Test
    void shouldJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.join(otherInput,
                        (v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.join(otherInput,
                        (k, v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add("bar", "");
            topology.input("other_input")
                    .at(0L)
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.leftJoin(otherInput,
                        (k, v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.input("other_input")
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add("bar", "");
            topology.input("other_input")
                    .at(0L)
                    .add("foo", "baz");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoin() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .add("foo", "baz");
            topology.input("input")
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add("bar", "");
            topology.input("input")
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinWithKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> otherInput = builder.stream("other_input");
                final KStreamX<String, String> joined = input.outerJoin(otherInput,
                        (k, v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)));
                joined.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .add("foo", "baz");
            topology.input("input")
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add("bar", "");
            topology.input("input")
                    .at(0L)
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("baz")
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("barbaz")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.join(otherInput,
                        Long::sum,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldJoinWithKeyUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.join(otherInput,
                        (k, v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add(2L, 0L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(0L)
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldLeftJoinWithKeyUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.leftJoin(otherInput,
                        (k, v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add(2L, 0L);
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(0L)
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add(2L, 0L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(0L)
                    .add(1L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(3L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldOuterJoinWithKeyUsingJoined() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KStreamX<Long, Long> joined = input.outerJoin(otherInput,
                        (k, v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        StreamJoinedX.with(Serdes.Long(), Serdes.Long(), Serdes.Long()));
                joined.to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("other_input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 3L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(Duration.ofMinutes(1L).toMillis() + 1) // trigger flush
                    .add(2L, 0L);
            topology.input("input")
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .at(0L)
                    .add(1L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(3L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(5L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGroupByKey() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, Long> count = grouped.count();
                count.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("foo", "baz");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGroupByKeyUsingGrouped() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KGroupedStreamX<Long, Long> grouped =
                        input.groupByKey(GroupedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> count = grouped.count();
                count.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(1L, 3L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey(1L)
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGroupBy() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupBy((k, v) -> v);
                final KTableX<String, Long> count = grouped.count();
                count.toStream().to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .add("foo", "bar")
                    .add("baz", "bar");
            topology.streamOutput()
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey("bar")
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldGroupByUsingGrouped() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input =
                        builder.stream("input", ConsumedX.with(Serdes.Long(), Serdes.Long()));
                final KGroupedStreamX<Long, Long> grouped =
                        input.groupBy((k, v) -> v, GroupedX.with(Serdes.Long(), Serdes.Long()));
                final KTableX<Long, Long> count = grouped.count();
                count.toStream().to("output", ProducedX.with(Serdes.Long(), Serdes.Long()));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .add(1L, 2L)
                    .add(3L, 2L);
            topology.streamOutput()
                    .withKeySerde(Serdes.Long())
                    .withValueSerde(Serdes.Long())
                    .expectNextRecord()
                    .hasKey(2L)
                    .hasValue(1L)
                    .expectNextRecord()
                    .hasKey(2L)
                    .hasValue(2L)
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMerge() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input1 = builder.stream("input1");
                final KStreamX<String, String> input2 = builder.stream("input2");
                final KStreamX<String, String> grouped = input1.merge(input2);
                grouped.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input1")
                    .add("foo", "bar");
            topology.input("input2")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldMergeNamed() {
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input1 = builder.stream("input1");
                final KStreamX<String, String> input2 = builder.stream("input2");
                final KStreamX<String, String> grouped = input1.merge(input2, Named.as("merge"));
                grouped.to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input1")
                    .add("foo", "bar");
            topology.input("input2")
                    .add("baz", "qux");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNextRecord()
                    .hasKey("baz")
                    .hasValue("qux")
                    .expectNoMoreRecord();
        }
    }

    @Test
    void shouldDoForEach() {
        final ForeachAction<String, String> action = mock();
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.foreach(action);
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            verify(action).apply("foo", "bar");
            verifyNoMoreInteractions(action);
        }
    }

    @Test
    void shouldDoForEachNamed() {
        final ForeachAction<String, String> action = mock();
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.foreach(action, Named.as("forEach"));
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            verify(action).apply("foo", "bar");
            verifyNoMoreInteractions(action);
        }
    }

    @Test
    void shouldPeek() {
        final ForeachAction<String, String> action = mock();
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.peek(action).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            verify(action).apply("foo", "bar");
            verifyNoMoreInteractions(action);
        }
    }

    @Test
    void shouldPeekNamed() {
        final ForeachAction<String, String> action = mock();
        final StringApp app = new StringApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.peek(action, Named.as("peek")).to("output");
            }
        };
        try (final TestTopology<String, String> topology = app.startApp()) {
            topology.input("input")
                    .add("foo", "bar");
            topology.streamOutput()
                    .expectNextRecord()
                    .hasKey("foo")
                    .hasValue("bar")
                    .expectNoMoreRecord();
            verify(action).apply("foo", "bar");
            verifyNoMoreInteractions(action);
        }
    }

    private abstract static class SimpleProcessor<KIn, VIn, KOut, VOut> implements Processor<KIn, VIn, KOut, VOut> {
        private ProcessorContext<KOut, VOut> context = null;

        @Override
        public void init(final ProcessorContext<KOut, VOut> context) {
            this.context = context;
        }

        protected void forward(final Record<? extends KOut, ? extends VOut> outputRecord) {
            this.context.forward(outputRecord);
        }

        protected <S extends StateStore> S getStateStore(final String name) {
            return this.context.getStateStore(name);
        }

    }

    private abstract static class SimpleFixedKeyProcessor<KIn, VIn, VOut> implements FixedKeyProcessor<KIn, VIn, VOut> {
        private FixedKeyProcessorContext<KIn, VOut> context = null;

        @Override
        public void init(final FixedKeyProcessorContext<KIn, VOut> context) {
            this.context = context;
        }

        protected void forward(final FixedKeyRecord<? extends KIn, ? extends VOut> outputRecord) {
            this.context.forward(outputRecord);
        }

        protected <S extends StateStore> S getStateStore(final String name) {
            return this.context.getStateStore(name);
        }

    }
}
