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

package com.bakdata.kafka.producer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaProducerApplicationCliTest {

    @Test
    void shouldParseArguments() {
        try (final KafkaProducerApplication<?> app = new KafkaProducerApplication<>() {
            @Override
            public ProducerApp createApp() {
                return new ProducerApp() {
                    @Override
                    public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SerializerConfig defaultSerializationConfig() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        }) {
            app.startApplicationWithoutExit(new String[]{
                    "--bootstrap-server", "bootstrap-servers",
                    "--schema-registry-url", "schema-registry",
                    "--output-topic", "output1",
                    "--labeled-output-topics", "label1=output2,label2=output3",
                    "--kafka-config", "foo=1,bar=2",
            });
            assertThat(app.getBootstrapServers()).isEqualTo("bootstrap-servers");
            assertThat(app.getSchemaRegistryUrl()).isEqualTo("schema-registry");
            assertThat(app.getOutputTopic()).isEqualTo("output1");
            assertThat(app.getLabeledOutputTopics())
                    .hasSize(2)
                    .containsEntry("label1", "output2")
                    .containsEntry("label2", "output3");
            assertThat(app.getKafkaConfig())
                    .hasSize(2)
                    .containsEntry("foo", "1")
                    .containsEntry("bar", "2");
        }
    }
}
