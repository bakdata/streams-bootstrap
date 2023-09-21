/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.test_applications.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

@ExtendWith(SoftAssertionsExtension.class)
class WordCountTest {
    private static final String[] ARGS = {
            "--input-topics", "Input,Input2",
            "--output-topic", "Output",
            "--brokers", "localhost:9092",
            "--streams-config", "test.ack=1,test1.ack=2"
    };
    private final WordCount app = CommandLine.populateCommand(new WordCount(), ARGS);
    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology = new TestTopologyExtension<>(this.app::createTopology,
            this.app.getKafkaProperties());
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldAggregateSameWordStream() {
        this.testTopology.input("Input")
                .add("bla")
                .add("blub")
                .add("bla");

        this.testTopology.streamOutput().withValueSerde(Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldSetKafkaProperties() {
        this.softly.assertThat(this.app.getKafkaProperties()).containsEntry("test.ack", "1");
        this.softly.assertThat(this.app.getKafkaProperties()).containsEntry("test1.ack", "2");
    }

    @Test
    void shouldSetDefaultSerdeWhenSchemaRegistryUrlIsNotSet() {
        this.softly.assertThat(this.app.getKafkaProperties())
                .containsEntry(DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        this.softly.assertThat(this.app.getKafkaProperties())
                .containsEntry(DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
    }

    @Test
    void shouldParseMultipleInputTopics() {
        this.softly.assertThat(this.app.getInputTopics())
                .containsExactly("Input", "Input2");
    }
}
