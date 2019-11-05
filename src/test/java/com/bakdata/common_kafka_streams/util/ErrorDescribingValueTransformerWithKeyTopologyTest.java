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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorDescribingValueTransformerWithKeyTopologyTest extends ErrorCaptureTopologyTest {

    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();
    private ValueTransformerWithKey<Integer, String, Long> mapper = null;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Integer, Long> mapped =
                input.transformValues(() -> ErrorDescribingValueTransformerWithKey.describeErrors(this.mapper));
        mapped.to(OUTPUT_TOPIC, Produced.with(INTEGER_SERDE, LONG_SERDE));
    }

    @Test
    void shouldNotAllowNullTransformer(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorDescribingValueTransformerWithKey.describeErrors(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotCaptureThrowable(final SoftAssertions softly) {
        final Error throwable = mock(Error.class);
        this.mapper = new ValueTransformerWithKey<>() {
            private ProcessorContext context = null;

            @Override
            public void close() {

            }

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Long transform(final Integer key, final String value) {
                if ("foo".equals(value)) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
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
        this.mapper = new ValueTransformerWithKey<>() {
            private ProcessorContext context = null;

            @Override
            public void close() {

            }

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Long transform(final Integer key, final String value) {
                if ("foo".equals(value)) {
                    throw new RuntimeException("Cannot process");
                }
                if ("bar".equals(value)) {
                    return 2L;
                }
                throw new UnsupportedOperationException();
            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar")
                .add(1, "foo"))
                .hasMessage("Cannot process ('" + ErrorUtil.toString(1) + "', '" + ErrorUtil.toString("foo") + "')");
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(INTEGER_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                );
    }

    @Test
    void shouldReturnOnNullInput(final SoftAssertions softly) {
        this.mapper = new ValueTransformerWithKey<>() {

            @Override
            public void close() {

            }

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public Long transform(final Integer key, final String value) {
                if (value == null) {
                    return 2L;
                }
                throw new UnsupportedOperationException();
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(INTEGER_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isNull())
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                );
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        this.mapper = new ValueTransformerWithKey<>() {

            @Override
            public void close() {

            }

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public Long transform(final Integer key, final String value) {
                if (value == null) {
                    throw new RuntimeException("Cannot process");
                }
                throw new UnsupportedOperationException();
            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null))
                .hasMessage("Cannot process ('" + ErrorUtil.toString(null) + "', '" + ErrorUtil.toString(null) + "')");
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(INTEGER_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();
    }

    @Test
    void shouldHandleReturnedNullValue(final SoftAssertions softly) {
        this.mapper = new ValueTransformerWithKey<>() {
            private ProcessorContext context = null;

            @Override
            public void close() {

            }

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Long transform(final Integer key, final String value) {
                if ("bar".equals(value)) {
                    return null;
                }
                throw new UnsupportedOperationException();
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar");
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(INTEGER_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2))
                        .extracting(ProducerRecord::value)
                        .satisfies(value -> softly.assertThat(value).isNull())
                );
    }
}
