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


import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.Runner;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.producer.apps.StringProducer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
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
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        return new ConfiguredProducerApp<>(new StringProducer(), new ProducerAppConfiguration(topics));
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        app.createCleanUpRunner().clean();
    }

    private static void run(final ExecutableApp<? extends Runner, ?, ?> executableApp) {
        executableApp.createRunner().run();
    }

    @Test
    void shouldDeleteTopic() {
        try (final ConfiguredProducerApp<ProducerApp> app = createStringApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app
                        .withRuntimeConfiguration(this.createConfig())) {
            run(executableApp);

            final List<KeyValue<String, String>> output = this.readOutputTopic(app.getTopics().getOutputTopic());
            this.softly.assertThat(output)
                    .containsExactlyInAnyOrderElementsOf(List.of(new KeyValue<>("foo", "bar")));

            clean(executableApp);

            try (final AdminClientX admin = this.newTestClient().admin()) {
                this.softly.assertThat(admin.topics().topic(app.getTopics().getOutputTopic()).exists())
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        try (final ConfiguredProducerApp<ProducerApp> app = this.createCleanUpHookApplication();
                final ExecutableProducerApp<ProducerApp> executableApp = app
                        .withRuntimeConfiguration(this.createConfig())) {
            clean(executableApp);
            verify(this.topicHook).deleted(app.getTopics().getOutputTopic());
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    private ConfiguredProducerApp<ProducerApp> createCleanUpHookApplication() {
        return new ConfiguredProducerApp<>(new StringProducer() {
            @Override
            public ProducerCleanUpConfiguration setupCleanUp(
                    final AppConfiguration<ProducerTopicConfig> configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(ProducerCleanUpRunnerTest.this.topicHook);
            }
        }, new ProducerAppConfiguration(ProducerTopicConfig.builder()
                .outputTopic("output")
                .build()));
    }

    private List<KeyValue<String, String>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, String>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(TestHelper::toKeyValue)
                .toList();
    }

}
