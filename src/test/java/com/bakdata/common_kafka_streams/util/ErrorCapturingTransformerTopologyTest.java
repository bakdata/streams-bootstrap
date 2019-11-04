/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

package com.bakdata.common_kafka_streams.util;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorCapturingTransformerTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    private Transformer<Integer, String, KeyValue<Double, Long>> mapper = null;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mapped =
                input.transform(() -> ErrorCapturingTransformer.captureErrors(this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .mapValues(error -> error.createDeadLetter("Description"))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldForwardSchemaRegistryTimeout(final SoftAssertions softly) {
        final RuntimeException throwable = createSchemaRegistryTimeoutException();
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (1 == key && "foo".equals(value)) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo"))
                .hasCauseInstanceOf(SerializationException.class);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldNotCaptureThrowable(final SoftAssertions softly) {
        final Error throwable = mock(Error.class);
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (1 == key && "foo".equals(value)) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureTransformerError(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (1 == key && "foo".equals(value)) {
                    throw new RuntimeException("Cannot process");
                }
                if (2 == key && "bar".equals(value)) {
                    return KeyValue.pair(2.0, 2L);
                }
                if (3 == key && "baz".equals(value)) {
                    this.context.forward(3.0, 3L);
                    return null;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar")
                .add(3, "baz");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(2)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                )
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(3.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(3L))
                );
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(1))
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetter.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).isEqualTo("foo");
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    softly.assertThat(deadLetter.getCause().getMessage()).isEqualTo("Cannot process");
                    softly.assertThat(deadLetter.getCause().getStackTrace()).isNotNull();
                });
    }

    @Test
    void shouldReturnOnNullInput(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (key == null && value == null) {
                    return KeyValue.pair(2.0, 2L);
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                );
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (key == null && value == null) {
                    throw new RuntimeException("Cannot process");
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isNull())
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetter.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).isNull();
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    softly.assertThat(deadLetter.getCause().getMessage()).isEqualTo("Cannot process");
                    softly.assertThat(deadLetter.getCause().getStackTrace()).isNotNull();
                });
    }

    @Test
    void shouldForwardOnNullInput(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (key == null && value == null) {
                    this.context.forward(3.0, 3L);
                    return null;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(3.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(3L))
                );
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleReturnedNullKeyValue(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (2 == key && "bar".equals(value)) {
                    return KeyValue.pair(null, null);
                }
                if (3 == key && "baz".equals(value)) {
                    this.context.forward(3.0, 3L);
                    return null;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar")
                .add(3, "baz");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(2)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isNull())
                        .extracting(ProducerRecord::value)
                        .satisfies(value -> softly.assertThat(value).isNull())
                )
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(3.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(3L))
                );
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleForwardedNullKeyValue(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Double, Long> transform(final Integer key, final String value) {
                if (2 == key && "bar".equals(value)) {
                    return KeyValue.pair(2.0, 2L);
                }
                if (3 == key && "baz".equals(value)) {
                    this.context.forward(null, null);
                    return null;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar")
                .add(3, "baz");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(2)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                )
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isNull())
                        .extracting(ProducerRecord::value)
                        .satisfies(value -> softly.assertThat(value).isNull())
                );
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

}
