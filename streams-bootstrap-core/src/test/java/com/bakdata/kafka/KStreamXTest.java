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
                final KStreamX<String, String> input = builder.stream("input");
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
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toOutputTopic(AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
                final KStreamX<String, String> input = builder.stream("input");
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
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toOutputTopic("label", AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
                final KStreamX<String, String> input = builder.stream("input");
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
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                input.toErrorTopic(AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
    void shouldMap() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(new KeyValue<>("baz", "qux"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.map(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldMapNamed() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(new KeyValue<>("baz", "qux"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.map(mapper, Named.as("map")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldMapValues() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldMapValuesNamed() {
        final ValueMapper<String, String> mapper = mock();
        when(mapper.apply("bar")).thenReturn("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper, Named.as("map")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldMapValuesWithKey() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldMapValuesWithKeyNamed() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn("baz");
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.mapValues(mapper, Named.as("map")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMap() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of(new KeyValue<>("baz", "qux")));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMap(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMapNamed() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of(new KeyValue<>("baz", "qux")));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMap(mapper, Named.as("flatMap")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMapValues() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        when(mapper.apply("bar")).thenReturn(List.of("baz"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMapValuesNamed() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        when(mapper.apply("bar")).thenReturn(List.of("baz"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper, Named.as("flatMap")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMapValuesWithKey() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of("baz"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFlatMapValuesWithKeyNamed() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        when(mapper.apply("foo", "bar")).thenReturn(List.of("baz"));
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.flatMapValues(mapper, Named.as("flatMap")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.process(processor);
                processed.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.process(processor, Named.as("process"));
                processed.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("baz")
                .hasValue("qux")
                .expectNoMoreRecord();
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        final KeyValueStore<String, String> store =
                topology.getTestDriver().getKeyValueStore("my-store");
        this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        final KeyValueStore<String, String> store =
                topology.getTestDriver().getKeyValueStore("my-store");
        this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.processValues(processor);
                processed.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> processed = input.processValues(processor, Named.as("process"));
                processed.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("baz")
                .expectNoMoreRecord();
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        final KeyValueStore<String, String> store =
                topology.getTestDriver().getKeyValueStore("my-store");
        this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        topology.stop();
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
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input().add("foo", "bar");
        final KeyValueStore<String, String> store =
                topology.getTestDriver().getKeyValueStore("my-store");
        this.softly.assertThat(store.get("foo")).isEqualTo("bar");
        topology.stop();
    }

    @Test
    void shouldFilter() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("foo", "baz")).thenReturn(false);
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filter(predicate).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFilterNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(true);
        when(predicate.test("foo", "baz")).thenReturn(false);
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filter(predicate, Named.as("filter")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFilterNot() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("foo", "baz")).thenReturn(true);
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filterNot(predicate).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldFilterNotNamed() {
        final Predicate<String, String> predicate = mock();
        when(predicate.test("foo", "bar")).thenReturn(false);
        when(predicate.test("foo", "baz")).thenReturn(true);
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.filterNot(predicate, Named.as("filterNot")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldSelectKey() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.selectKey((k, v) -> v).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("bar")
                .hasValue("bar")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldSelectKeyNamed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.selectKey((k, v) -> v, Named.as("select")).to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input()
                .add("foo", "bar");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("bar")
                .hasValue("bar")
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldMapCapturingErrorsNamed() {
        final KeyValueMapper<String, String, KeyValue<String, String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(KeyValue.pair("success_key", "success_value")).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldMapValuesCapturingErrorsNamed() {
        final ValueMapper<String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn("success").when(mapper).apply("baz");
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldMapValuesWithKeyCapturingErrorsNamed() {
        final ValueMapperWithKey<String, String, String> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn("success").when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldFlatMapCapturingErrorsNamed() {
        final KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of(KeyValue.pair("success_key", "success_value"))).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldFlatMapValuesCapturingErrorsNamed() {
        final ValueMapper<String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("bar");
        doReturn(List.of("success")).when(mapper).apply("baz");
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
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
    void shouldFlatMapValuesWithKeyCapturingErrorsNamed() {
        final ValueMapperWithKey<String, String, Iterable<String>> mapper = mock();
        doThrow(new RuntimeException("Cannot process")).when(mapper).apply("foo", "bar");
        doReturn(List.of("success")).when(mapper).apply("foo", "baz");
        final StreamsApp app = new SimpleApp() {
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
        final StreamsApp app = new SimpleApp() {
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
        final StreamsApp app = new SimpleApp() {
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
        final StreamsApp app = new SimpleApp() {
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
        final StreamsApp app = new SimpleApp() {
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
                final KStreamX<String, String> input = builder.stream("input");
                final KStreamX<String, String> repartitioned = input.repartition();
                repartitioned.to("output");
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
    void shouldRepartitionUsingRepartitioned() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> repartitioned = input.repartition(
                        AutoRepartitioned.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                repartitioned.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
                final KStreamX<String, String> input = builder.stream("input");
                final KTableX<String, String> table = input.toTable();
                table.toStream().to("output");
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
    void shouldConvertToTableNamed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KTableX<String, String> table = input.toTable(Named.as("toTable"));
                table.toStream().to("output");
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
    void shouldConvertToTableUsingMaterialized() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KTableX<Long, Long> table = input.toTable(
                        AutoMaterialized.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                table.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
    void shouldConvertToTableNamedUsingMaterialized() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KTableX<Long, Long> table = input.toTable(Named.as("toTable"),
                        AutoMaterialized.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                table.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
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
    void shouldJoin() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input("input")
                .add("foo", "bar");
        topology.input("other_input")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("barbaz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldJoinWithKey() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input("input")
                .add("foo", "bar");
        topology.input("other_input")
                .add("foo", "baz");
        topology.streamOutput()
                .expectNextRecord()
                .hasKey("foo")
                .hasValue("barbaz")
                .expectNoMoreRecord();
        topology.stop();
    }

    @Test
    void shouldLeftJoin() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldLeftJoinWithKey() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldOuterJoin() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldOuterJoinWithKey() {
        final StreamsApp app = new SimpleApp() {
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
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldJoinUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.join(otherInput,
                        Long::sum,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldJoinWithKeyUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.join(otherInput,
                        (k, v1, v2) -> v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldLeftJoinUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.leftJoin(otherInput,
                        (v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldLeftJoinWithKeyUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.leftJoin(otherInput,
                        (k, v1, v2) -> v2 == null ? v1 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldOuterJoinUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.outerJoin(otherInput,
                        (v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldOuterJoinWithKeyUsingJoined() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> otherInput = builder.stream("other_input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KStreamX<Long, Long> joined = input.outerJoin(otherInput,
                        (k, v1, v2) -> v1 == null ? v2 : v1 + v2,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1L)),
                        AutoStreamJoined.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long()), Preconfigured.create(Serdes.Long())));
                joined.to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldGroupByKey() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupByKey();
                final KTableX<String, Long> count = grouped.count();
                count.toStream().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldGroupByKeyUsingGrouped() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KGroupedStreamX<Long, Long> grouped = input.groupByKey(
                        AutoGrouped.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KTableX<Long, Long> count = grouped.count();
                count.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldGroupBy() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                final KGroupedStreamX<String, String> grouped = input.groupBy((k, v) -> v);
                final KTableX<String, Long> count = grouped.count();
                count.toStream().to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldGroupByUsingGrouped() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<Long, Long> input = builder.stream("input",
                        AutoConsumed.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KGroupedStreamX<Long, Long> grouped = input.groupBy((k, v) -> v,
                        AutoGrouped.with(Preconfigured.create(Serdes.Long()),
                                Preconfigured.create(Serdes.Long())));
                final KTableX<Long, Long> count = grouped.count();
                count.toStream().to("output", AutoProduced.with(Preconfigured.create(Serdes.Long()),
                        Preconfigured.create(Serdes.Long())));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldMerge() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input1 = builder.stream("input1");
                final KStreamX<String, String> input2 = builder.stream("input2");
                final KStreamX<String, String> grouped = input1.merge(input2);
                grouped.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldMergeNamed() {
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input1 = builder.stream("input1");
                final KStreamX<String, String> input2 = builder.stream("input2");
                final KStreamX<String, String> grouped = input1.merge(input2, Named.as("merge"));
                grouped.to("output");
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
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
        topology.stop();
    }

    @Test
    void shouldDoForEach() {
        final ForeachAction<String, String> action = mock();
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.foreach(action);
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input("input")
                .add("foo", "bar");
        verify(action).apply("foo", "bar");
        verifyNoMoreInteractions(action);
        topology.stop();
    }

    @Test
    void shouldDoForEachNamed() {
        final ForeachAction<String, String> action = mock();
        final StreamsApp app = new SimpleApp() {
            @Override
            public void buildTopology(final TopologyBuilder builder) {
                final KStreamX<String, String> input = builder.stream("input");
                input.foreach(action, Named.as("forEach"));
            }
        };
        final TestTopology<String, String> topology =
                startApp(app, StreamsTopicConfig.builder().build());
        topology.input("input")
                .add("foo", "bar");
        verify(action).apply("foo", "bar");
        verifyNoMoreInteractions(action);
        topology.stop();
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
