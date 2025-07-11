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

package com.bakdata.kafka.streams;


import static com.bakdata.kafka.TestHelper.clean;
import static com.bakdata.kafka.TestHelper.createExecutableApp;
import static com.bakdata.kafka.TestHelper.run;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.TestSchemaRegistry;
import com.bakdata.kafka.streams.apps.MirrorKeyWithAvro;
import com.bakdata.kafka.streams.apps.MirrorValueWithAvro;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class SchemaRegistryStreamsAppTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    private final TestSchemaRegistry schemaRegistry = new TestSchemaRegistry();

    private static ConfiguredStreamsApp<StreamsApp> createMirrorValueApplication() {
        final StreamsApp app = new MirrorValueWithAvro();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorKeyApplication() {
        final StreamsApp app = new MirrorKeyWithAvro();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    @Test
    void shouldDeleteValueSchema()
            throws IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorValueApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = createExecutableApp(app,
                        this.createConfigWithSchemaRegistry());
                final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClientWithSchemaRegistry();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, testRecord)
                    ));

            run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            final String outputTopic = app.getTopics().getOutputTopic();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(outputTopic + "-value", inputTopic + "-value");
            clean(executableApp);
            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(outputTopic + "-value")
                    .contains(inputTopic + "-value");
        }
    }

    @Test
    void shouldDeleteKeySchema()
            throws IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = createExecutableApp(app,
                        this.createConfigWithSchemaRegistry());
                final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClientWithSchemaRegistry();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new SpecificAvroSerializer<>())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(testRecord, "val")
                    ));

            run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            final String outputTopic = app.getTopics().getOutputTopic();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(outputTopic + "-key", inputTopic + "-key");
            clean(executableApp);
            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(outputTopic + "-key")
                    .contains(inputTopic + "-key");
        }
    }

    private RuntimeConfiguration createConfigWithSchemaRegistry() {
        return this.schemaRegistry.configure(this.createConfig());
    }

    private KafkaTestClient newTestClientWithSchemaRegistry() {
        return new KafkaTestClient(this.createConfigWithSchemaRegistry());
    }

}
