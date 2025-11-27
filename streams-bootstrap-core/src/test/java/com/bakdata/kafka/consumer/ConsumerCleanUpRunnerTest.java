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

import static com.bakdata.kafka.TestHelper.clean;
import static com.bakdata.kafka.consumer.TestHelper.assertContent;
import static com.bakdata.kafka.consumer.TestHelper.createExecutableApp;
import static com.bakdata.kafka.consumer.TestHelper.reset;
import static com.bakdata.kafka.consumer.TestHelper.run;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
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
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerCleanUpRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

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

    private void assertSize(final Collection<ConsumerRecord<String, String>> records, final int expectedMessageCount) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> this.softly.assertThat(records).hasSize(expectedMessageCount));
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
            assertContent(this.softly, stringConsumer.getConsumedRecords(), expectedValues,
                    "Output contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueGroupId());
                this.softly.assertThat(consumerGroupClient.exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueGroupId());
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
            assertContent(this.softly, stringConsumer.getConsumedRecords(), expectedValues,
                    "Contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupsClient groups = adminClient.consumerGroups();
                this.softly.assertThat(groups.group(app.getUniqueGroupId()).exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

            awaitClosed(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupsClient groups = adminClient.consumerGroups();
                groups.group(app.getUniqueGroupId()).delete();
                this.softly.assertThat(groups.group(app.getUniqueGroupId()).exists())
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
            this.assertSize(stringConsumer.getConsumedRecords(), 3);
            awaitClosed(executableApp);

            run(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);
            awaitClosed(executableApp);

            reset(executableApp);

            run(executableApp);
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
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            runAsync(runner);
            // Wait until consumer application has consumed all data
            awaitActive(executableApp);
            // should throw exception because consumer group is still active
            this.softly.assertThatThrownBy(() -> reset(executableApp))
                    .isInstanceOf(CleanUpException.class)
                    .hasMessageContaining("Error resetting application, consumer group is not empty");
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
            this.assertSize(stringConsumer.getConsumedRecords(), 3);
            awaitClosed(executableApp);

            run(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);
            awaitClosed(executableApp);

            reset(executableApp);

            run(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 6);
        }
    }

    @Test
    void shouldNotThrowExceptionOnResetIfConsumerGroupNotExists() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig())) {
            // The app is not run so the consumer group is never created
            this.softly.assertThatCode(() -> reset(executableApp)).doesNotThrowAnyException();
        }
    }

}
