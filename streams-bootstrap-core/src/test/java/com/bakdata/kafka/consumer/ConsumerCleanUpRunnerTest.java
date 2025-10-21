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

package com.bakdata.kafka.consumer;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.Runner;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient;
import com.bakdata.kafka.admin.ConsumerGroupsClient.ConsumerGroupClient;
import com.bakdata.kafka.consumer.apps.StringConsumer;
import com.bakdata.kafka.consumer.apps.StringPatternConsumer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ConsumerCleanUpRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private TopicHook topicHook;

    static ConfiguredConsumerApp<ConsumerApp> createStringApplication() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        return new ConfiguredConsumerApp<>(new StringConsumer(), new ConsumerAppConfiguration(topics));
    }

    static ConfiguredConsumerApp<ConsumerApp> createStringPatternApplication() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputPattern(Pattern.compile(".*_topic"))
                .build();
        return new ConfiguredConsumerApp<>(new StringPatternConsumer(), new ConsumerAppConfiguration(topics));
    }

    private static void reset(final ExecutableApp<?, ConsumerCleanUpRunner, ?> app) {
        try (final ConsumerCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        app.createCleanUpRunner().clean();
    }

    private static Runner run(final ExecutableApp<? extends Runner, ?, ?> executableApp) {
        final Runner consumerRunner = executableApp.createRunner();
        new Thread(consumerRunner).start();
        return consumerRunner;
    }

    static ExecutableConsumerApp<ConsumerApp> createExecutableApp(final ConfiguredConsumerApp<ConsumerApp> app,
            final RuntimeConfiguration runtimeConfiguration) {
        return app.withRuntimeConfiguration(runtimeConfiguration);
    }

    private void assertContent(final Collection<ConsumerRecord<String, String>> consumedRecords,
            final Iterable<? extends KeyValue<String, String>> expectedValues, final String description) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    final List<KeyValue<String, String>> consumedKeyValues = consumedRecords
                            .stream()
                            .map(TestHelper::toKeyValue)
                            .toList();
                    this.softly.assertThat(consumedKeyValues)
                            .as(description)
                            .containsExactlyInAnyOrderElementsOf(expectedValues);
                });
    }

    private void assertSize(final Collection<ConsumerRecord<String, String>> records, final int expectedMessageCount) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    this.softly.assertThat(records).hasSize(expectedMessageCount);
                });
    }

    @Test
    void shouldDeleteConsumerGroup() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues =
                    List.of(new KeyValue<>("blub", "blub"),
                            new KeyValue<>("bla", "bla"),
                            new KeyValue<>("blub", "blub")
                    );

            final StringConsumer stringConsumer = (StringConsumer) app.app();

            run(executableApp);
            awaitActive(executableApp);
            this.assertContent(stringConsumer.getConsumedRecords(), expectedValues,
                    "Output contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueAppId());
                this.softly.assertThat(consumerGroupClient.exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

            stringConsumer.shutdown();
            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueAppId());
                this.softly.assertThat(consumerGroupClient.exists())
                        .as("Consumer group is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldNotThrowAnErrorIfConsumerGroupDoesNotExist() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues =
                    List.of(new KeyValue<>("blub", "blub"),
                            new KeyValue<>("bla", "bla"),
                            new KeyValue<>("blub", "blub")
                    );

            final StringConsumer stringConsumer = (StringConsumer) app.app();

            run(executableApp);
            awaitActive(executableApp);
            this.assertContent(stringConsumer.getConsumedRecords(), expectedValues,
                    "Contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupsClient groups = adminClient.consumerGroups();
                this.softly.assertThat(groups.group(app.getUniqueAppId()).exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

            stringConsumer.shutdown();
            awaitClosed(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupsClient groups = adminClient.consumerGroups();
                groups.group(app.getUniqueAppId()).delete();
                this.softly.assertThat(groups.group(app.getUniqueAppId()).exists())
                        .as("Consumer group is deleted")
                        .isFalse();
            }
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecords() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final StringConsumer stringConsumer = (StringConsumer) app.app();

            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            // Wait until all applications are completely stopped before triggering cleanup
            stringConsumer.shutdown();
            awaitClosed(executableApp);
            reset(executableApp);

            stringConsumer.start();
            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 6);
        }
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopic() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldThrowExceptionOnResetterError() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            run(executableApp);
            // Wait until consumer application has consumed all data
            awaitActive(executableApp);
            // should throw exception because consumer group is still active
            this.softly.assertThatThrownBy(() -> reset(executableApp))
                    .isInstanceOf(CleanUpException.class)
                    .hasMessageContaining("Error running streams resetter. Exit code 1");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithPattern() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringPatternApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final String topic = "input_topic";
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(topic);
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(topic, List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final StringPatternConsumer stringConsumer = (StringPatternConsumer) app.app();

            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            // Wait until all applications are completely stopped before triggering cleanup
            stringConsumer.shutdown();
            awaitClosed(executableApp);
            reset(executableApp);

            stringConsumer.start();
            run(executableApp);
            awaitProcessing(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 6);
        }
    }

}
