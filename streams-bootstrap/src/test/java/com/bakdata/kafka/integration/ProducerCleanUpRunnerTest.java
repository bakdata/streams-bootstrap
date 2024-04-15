/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

package com.bakdata.kafka.integration;


import static com.bakdata.kafka.integration.ProducerRunnerTest.configureApp;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ConfiguredProducerApp;
import com.bakdata.kafka.EffectiveProducerAppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.ExecutableProducerApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.ProducerApp;
import com.bakdata.kafka.ProducerCleanUpConfiguration;
import com.bakdata.kafka.ProducerTopicConfig;
import com.bakdata.kafka.Runner;
import com.bakdata.kafka.test_applications.AvroKeyProducer;
import com.bakdata.kafka.test_applications.AvroValueProducer;
import com.bakdata.kafka.test_applications.StringProducer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ProducerCleanUpRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private TopicHook topicHook;

    static ConfiguredProducerApp<ProducerApp> createStringApplication() {
        return configureApp(new StringProducer(), ProducerTopicConfig.builder()
                .outputTopic("output")
                .build());
    }

    private static ConfiguredProducerApp<ProducerApp> createAvroKeyApplication() {
        return configureApp(new AvroKeyProducer(), ProducerTopicConfig.builder()
                .outputTopic("output")
                .build());
    }

    private static ConfiguredProducerApp<ProducerApp> createAvroValueApplication() {
        return configureApp(new AvroValueProducer(), ProducerTopicConfig.builder()
                .outputTopic("output")
                .build());
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        app.createCleanUpRunner().clean();
    }

    private static void run(final ExecutableApp<? extends Runner, ?, ?> executableApp) {
        executableApp.createRunner().run();
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        try (final ConfiguredProducerApp<ProducerApp> app = createStringApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            run(executableApp);

            final List<KeyValue<String, String>> output = this.readOutputTopic(app.getTopics().getOutputTopic());
            this.softly.assertThat(output)
                    .containsExactlyInAnyOrderElementsOf(List.of(new KeyValue<>("foo", "bar")));

            clean(executableApp);

            this.softly.assertThat(this.kafkaCluster.exists(app.getTopics().getOutputTopic()))
                    .as("Output topic is deleted")
                    .isFalse();
        }
    }

    @Test
    void shouldDeleteValueSchema() throws IOException, RestClientException {
        try (final ConfiguredProducerApp<ProducerApp> app = createAvroValueApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
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
                final ExecutableProducerApp<ProducerApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
            run(executableApp);

            final String outputTopic = app.getTopics().getOutputTopic();
            this.softly.assertThat(client.getAllSubjects())
                    .contains(outputTopic + "-key");
            clean(executableApp);
            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(outputTopic + "-key");
        }
    }

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        try (final ConfiguredProducerApp<ProducerApp> app = this.createCleanUpHookApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app.withEndpoint(this.createEndpoint())) {
            clean(executableApp);
            verify(this.topicHook).deleted(app.getTopics().getOutputTopic());
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    private ConfiguredProducerApp<ProducerApp> createCleanUpHookApplication() {
        return configureApp(new StringProducer() {
            @Override
            public ProducerCleanUpConfiguration setupCleanUp(final EffectiveProducerAppConfiguration configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(ProducerCleanUpRunnerTest.this.topicHook);
            }
        }, ProducerTopicConfig.builder()
                .outputTopic("output")
                .build());
    }

    private List<KeyValue<String, String>> readOutputTopic(final String outputTopic) throws InterruptedException {
        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from(outputTopic).build();
        return this.kafkaCluster.read(readRequest);
    }

}
