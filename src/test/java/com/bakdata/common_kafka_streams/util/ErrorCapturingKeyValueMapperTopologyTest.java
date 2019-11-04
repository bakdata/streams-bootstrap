package com.bakdata.common_kafka_streams.util;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(SoftAssertionsExtension.class)
class ErrorCapturingKeyValueMapperTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    @Mock
    KeyValueMapper<Integer, String, KeyValue<Double, Long>> mapper;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mapped =
                input.map(ErrorCapturingKeyValueMapper.captureErrors(this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .mapValues(error -> error.createDeadLetter("Description"))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldForwardSchemaRegistryTimeout(final SoftAssertions softly) {
        when(this.mapper.apply(1, "foo")).thenThrow(createSchemaRegistryTimeoutException());
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
        final Throwable throwable = mock(Error.class);
        when(this.mapper.apply(1, "foo")).thenThrow(throwable);
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureKeyValueMapperError(final SoftAssertions softly) {
        doThrow(new RuntimeException("Cannot process")).when(this.mapper).apply(1, "foo");
        doReturn(KeyValue.pair(2.0, 2L)).when(this.mapper).apply(2, "bar");
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2.0))
                .extracting(ProducerRecord::value)
                .isInstanceOf(Long.class)
                .satisfies(value -> softly.assertThat(value).isEqualTo(2L));
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
    void shouldHandleNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null, null)).thenReturn(KeyValue.pair(2.0, 2L));
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
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(2.0))
                .extracting(ProducerRecord::value)
                .isInstanceOf(Long.class)
                .satisfies(value -> softly.assertThat(value).isEqualTo(2L));
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null, null)).thenThrow(new RuntimeException("Cannot process"));
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
    void shouldHandleNullKeyValue(final SoftAssertions softly) {
        when(this.mapper.apply(2, "bar")).thenReturn(KeyValue.pair(null, null));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isNull())
                .extracting(ProducerRecord::value)
                .satisfies(value -> softly.assertThat(value).isNull());
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

}
