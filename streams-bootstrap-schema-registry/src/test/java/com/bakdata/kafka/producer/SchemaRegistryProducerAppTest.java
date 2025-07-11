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


import static com.bakdata.kafka.TestHelper.clean;
import static com.bakdata.kafka.TestHelper.run;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.producer.apps.AvroKeyProducer;
import com.bakdata.kafka.producer.apps.AvroValueProducer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class SchemaRegistryProducerAppTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static ConfiguredProducerApp<ProducerApp> createAvroKeyApplication() {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        return new ConfiguredProducerApp<>(new AvroKeyProducer(), new ProducerAppConfiguration(topics));
    }

    private static ConfiguredProducerApp<ProducerApp> createAvroValueApplication() {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        return new ConfiguredProducerApp<>(new AvroValueProducer(), new ProducerAppConfiguration(topics));
    }

    @Test
    void shouldDeleteValueSchema() throws IOException, RestClientException {
        try (final ConfiguredProducerApp<ProducerApp> app = createAvroValueApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app
                        .withRuntimeConfiguration(this.createConfig());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            run(executableApp);

            final String outputTopic = app.getTopics().getOutputTopic();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(outputTopic + "-value");
            clean(executableApp);
            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(outputTopic + "-value");
        }
    }

    @Test
    void shouldDeleteKeySchema() throws IOException, RestClientException {
        try (final ConfiguredProducerApp<ProducerApp> app = createAvroKeyApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app
                        .withRuntimeConfiguration(this.createConfig());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            run(executableApp);

            final String outputTopic = app.getTopics().getOutputTopic();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(outputTopic + "-key");
            clean(executableApp);
            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(outputTopic + "-key");
        }
    }

}
