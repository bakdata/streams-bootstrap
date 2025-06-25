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

package com.bakdata.kafka.integration;


import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ConfiguredConsumerApp;
import com.bakdata.kafka.ConfiguredConsumerApp;
import com.bakdata.kafka.ConfiguredConsumerApp;
import com.bakdata.kafka.ConsumerApp;
import com.bakdata.kafka.ConsumerCleanUpConfiguration;
import com.bakdata.kafka.ConsumerCleanUpRunner;
import com.bakdata.kafka.ConsumerTopicConfig;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.ExecutableConsumerApp;
import com.bakdata.kafka.ExecutableConsumerApp;
import com.bakdata.kafka.ExecutableConsumerApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.Runner;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.ConsumerApp;
import com.bakdata.kafka.ConsumerRunner;
import com.bakdata.kafka.ConsumerApp;
import com.bakdata.kafka.StreamsCleanUpRunner;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import com.bakdata.kafka.test_applications.StringConsumer;
import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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
        return new ConfiguredConsumerApp<>(new StringConsumer("input"), topics);
    }

    // TODO
//    private static ConfiguredConsumerApp<ConsumerApp> createAvroKeyApplication() {
//        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
//                .inputTopics(List.of("input"))
//                .build();
//        return new ConfiguredConsumerApp<>(new AvroKeyConsumer(), topics);
//    }
//
//    private static ConfiguredConsumerApp<ConsumerApp> createAvroValueApplication() {
//        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
//                .inputTopics(List.of("input"))
//                .build();
//        return new ConfiguredConsumerApp<>(new AvroValueConsumer(), topics);
//    }

    private static void reset(final ExecutableApp<?, ConsumerCleanUpRunner, ?> app) {
        try (final ConsumerCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        app.createCleanUpRunner().clean();
    }

    // TODO use or modify TestHelper?
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
            final Iterable<? extends KeyValue<String, String>> expectedValues) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    final List<KeyValue<String, String>> consumedKeyValues = consumedRecords
                            .stream()
                            .map(StreamsCleanUpRunnerTest::toKeyValue)
                            .collect(Collectors.toList());
                    this.softly.assertThat(consumedKeyValues)
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
                        this.createConfigWithoutSchemaRegistry())) {
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

            final StringConsumer stringConsumer = (StringConsumer) app.getApp();

            run(executableApp);
            awaitActive(executableApp);
            this.assertContent(stringConsumer.getConsumedRecords(), expectedValues);

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            stringConsumer.shutdown();
            awaitClosed(executableApp);
            clean(executableApp);

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldNotThrowAnErrorIfConsumerGroupDoesNotExist() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
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

            final StringConsumer stringConsumer = (StringConsumer) app.getApp();

            run(executableApp);
            awaitActive(executableApp);
            this.assertContent(stringConsumer.getConsumedRecords(), expectedValues);

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            stringConsumer.shutdown();
            awaitClosed(executableApp);

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                consumerGroupClient.deleteConsumerGroup(app.getUniqueAppId());
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
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
                        this.createConfigWithoutSchemaRegistry())) {
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

            final StringConsumer stringConsumer = (StringConsumer) app.getApp();

            run(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            run(executableApp);
            this.assertSize(stringConsumer.getConsumedRecords(), 3);

            // Wait until all stream applications are completely stopped before triggering cleanup
            stringConsumer.shutdown();
            awaitClosed(executableApp);
            reset(executableApp);

            stringConsumer.start();
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

//    TODO
//    @Test
//    void shouldReprocessAlreadySeenRecordsWithPattern() {
//        try (final ConfiguredConsumerApp<ConsumerApp> app = createWordCountPatternApplication();
//                final ExecutableConsumerApp<ConsumerApp> executableApp = this.createExecutableApp(app,
//                        this.createConfigWithoutSchemaRegistry())) {
//            final KafkaTestClient testClient = this.newTestClient();
//            testClient.createTopic(app.getTopics().getOutputTopic());
//            testClient.send()
//                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                    .to("input_topic", List.of(
//                            new SimpleProducerRecord<>(null, "a"),
//                            new SimpleProducerRecord<>(null, "b")
//                    ));
//            testClient.send()
//                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                    .to("another_topic", List.of(
//                            new SimpleProducerRecord<>(null, "c")
//                    ));
//
//            run(executableApp);
//            this.assertSize(app.getTopics().getOutputTopic(), 3);
//            run(executableApp);
//            this.assertSize(app.getTopics().getOutputTopic(), 3);
//
//            // Wait until all Consumer application are completely stopped before triggering cleanup
//            awaitClosed(executableApp);
//            reset(executableApp);
//
//            run(executableApp);
//            this.assertSize(app.getTopics().getOutputTopic(), 6);
//        }
//    }

}
