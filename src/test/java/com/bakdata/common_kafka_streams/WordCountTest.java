/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.common_kafka_streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.test_applications.WordCount;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import picocli.CommandLine;

class WordCountTest {
    private static final String[] ARGS = {"--input-topics", "Input,Input2", "--output-topic", "Output",
            "--brokers", "localhost:9092", "--schema-registry-url", "registryUrl", "--streams-config",
            "test.ack=1,test1.ack=2"};
    private final WordCount app = CommandLine.populateCommand(new WordCount(), ARGS);

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology = new TestTopologyExtension<>(this.app::createTopology,
            this.app.getKafkaProperties());

    @Test
    void shouldAggregateSameWordStream() {
        this.testTopology.input("Input").add("bla")
                .add("blub")
                .add("bla");

        this.testTopology.streamOutput("Output").withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldSetKafkaProperties() {
        assertThat(this.app.getKafkaProperties().getProperty("test.ack")).isEqualTo("1");
        assertThat(this.app.getKafkaProperties().getProperty("test1.ack")).isEqualTo("2");
    }

    @Test
    void shouldParseMultipleInputTopics() {
        assertThat(this.app.getInputTopics())
                .containsExactly("Input", "Input2");
    }
}
