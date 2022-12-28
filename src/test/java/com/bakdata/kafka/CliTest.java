/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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

import static org.assertj.core.api.Assertions.assertThat;

import com.ginsberg.junit.exit.ExpectSystemExitWithStatus;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Test;

class CliTest {

    @Test
    @ExpectSystemExitWithStatus(0)
    void shouldExitWithSuccessCode() {
        KafkaStreamsApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public void buildTopology(final StreamsBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void run() {
                // do nothing
            }
        }, new String[]{
                "--brokers", "brokers",
                "--schema-registry-url", "schema-registry",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    @ExpectSystemExitWithStatus(1)
    void shouldExitWithErrorCode() {
        KafkaStreamsApplication.startApplication(new KafkaStreamsApplication() {
            @Override
            public void buildTopology(final StreamsBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void run() {
                throw new RuntimeException();
            }
        }, new String[]{
                "--brokers", "brokers",
                "--schema-registry-url", "schema-registry",
                "--input-topics", "input",
                "--output-topic", "output",
        });
    }

    @Test
    void shouldParseArguments() {
        final KafkaStreamsApplication app = new KafkaStreamsApplication() {
            @Override
            public void buildTopology(final StreamsBuilder builder) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getUniqueAppId() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void run() {
                //do nothing
            }
        };
        KafkaStreamsApplication.startApplications(app, new String[]{
                "--brokers", "brokers",
                "--schema-registry-url", "schema-registry",
                "--input-topics", "input1,input2",
                "--extra-input-topics", "role1=input3,role2=input4;input5",
                "--input-pattern", ".*",
                "--extra-input-patterns", "role1=.+,role2=\\d+",
                "--output-topic", "output1",
                "--extra-output-topics", "role1=output2,role2=output3",
        });
        assertThat(app.getInputTopics()).containsExactly("input1", "input2");
        assertThat(app.getExtraInputTopics())
                .hasSize(2)
                .containsEntry("role1", List.of("input3"))
                .containsEntry("role2", List.of("input4", "input5"));
        assertThat(app.getInputTopic("role2")).isEqualTo("input4");
        assertThat(app.getInputTopics("role2")).isEqualTo(List.of("input4", "input5"));
        assertThat(app.getInputPattern())
                .satisfies(pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".*").pattern()));
        assertThat(app.getExtraInputPatterns())
                .hasSize(2)
                .hasEntrySatisfying("role1",
                        pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile(".+").pattern()))
                .hasEntrySatisfying("role2",
                        pattern -> assertThat(pattern.pattern()).isEqualTo(Pattern.compile("\\d+").pattern()));
        assertThat(app.getOutputTopic()).isEqualTo("output1");
        assertThat(app.getExtraOutputTopics())
                .hasSize(2)
                .containsEntry("role1", "output2")
                .containsEntry("role2", "output3");
    }
}
