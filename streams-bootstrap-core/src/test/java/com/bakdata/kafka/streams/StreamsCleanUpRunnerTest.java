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
import static com.bakdata.kafka.TestHelper.reset;
import static com.bakdata.kafka.TestHelper.run;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupClient;
import com.bakdata.kafka.admin.TopicClient;
import com.bakdata.kafka.streams.apps.ComplexTopologyApplication;
import com.bakdata.kafka.streams.apps.Mirror;
import com.bakdata.kafka.streams.apps.WordCount;
import com.bakdata.kafka.streams.apps.WordCountPattern;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsCleanUpRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private TopicHook topicHook;
    @TempDir
    private Path stateDir;

    private static ConfiguredStreamsApp<StreamsApp> createWordCountPatternApplication() {
        final StreamsApp app = new WordCountPattern();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputPattern(Pattern.compile(".*_topic"))
                .outputTopic("word_output")
                .build()));
    }

    private static ConfiguredStreamsApp<StreamsApp> createWordCountApplication() {
        final StreamsApp app = new WordCount();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("word_input"))
                .outputTopic("word_output")
                .build()));
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorApplication() {
        final StreamsApp app = new Mirror();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    @Test
    void shouldDeleteTopic() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                this.softly.assertThat(topicClient.exists(app.getTopics().getOutputTopic()))
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldDeleteConsumerGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldNotThrowAnErrorIfConsumerGroupDoesNotExist() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            awaitClosed(executableApp);

            try (final AdminClientX adminClient = testClient.admin();
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
    void shouldDeleteInternalTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig())) {

            final String testRecord = "key 1";
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String uniqueAppId = app.getUniqueAppId();
            final String internalTopic =
                    uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition";
            final String backingTopic =
                    uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";

            try (final AdminClientX admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(internalTopic)).isTrue();
                this.softly.assertThat(topicClient.exists(backingTopic)).isTrue();
            }

            awaitClosed(executableApp);
            reset(executableApp);

            try (final AdminClientX admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(internalTopic)).isFalse();
                this.softly.assertThat(topicClient.exists(backingTopic)).isFalse();
            }
        }
    }

    @Test
    void shouldDeleteIntermediateTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig())) {

            final String testRecord = "key 1";
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

            try (final AdminClientX admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(manualTopic)).isTrue();
            }

            awaitClosed(executableApp);
            reset(executableApp);

            try (final AdminClientX admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(manualTopic)).isFalse();
            }
        }
    }

    @Test
    void shouldDeleteState() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "All entries are once in the input topic after the 1st run");

            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .toList();
            this.assertContent(app.getTopics().getOutputTopic(), entriesTwice,
                    "All entries are twice in the input topic after the 2nd run");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecords() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b"),
                            new SimpleProducerRecord<>(null, "c")
                    ));

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    @Test
    void shouldCallCleanupHookForInternalAndIntermediateTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexCleanUpHookApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig())) {
            reset(executableApp);
            final String uniqueAppId = app.getUniqueAppId();
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-changelog");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog");
            verify(this.topicHook).deleted(ComplexTopologyApplication.THROUGH_TOPIC);
            verify(this.topicHook).close();
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexCleanUpHookApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig())) {
            clean(executableApp);
            final String uniqueAppId = app.getUniqueAppId();
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-changelog");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog");
            verify(this.topicHook).deleted(ComplexTopologyApplication.THROUGH_TOPIC);
            verify(this.topicHook).deleted(app.getTopics().getOutputTopic());
            verify(this.topicHook).close();
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopic() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig())) {
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldThrowExceptionOnResetterError() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfig());
                final StreamsRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            runAsync(runner);
            // Wait until stream application has consumed all data
            awaitActive(executableApp);
            // should throw exception because consumer group is still active
            this.softly.assertThatThrownBy(() -> reset(executableApp))
                    .isInstanceOf(CleanUpException.class)
                    .hasMessageContaining("Error running streams resetter. Exit code 1");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithPattern() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountPatternApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createConfigWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to("input_topic", List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b")
                    ));
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to("another_topic", List.of(
                            new SimpleProducerRecord<>(null, "c")
                    ));

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all streams application are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    private ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app,
            final RuntimeConfiguration configuration) {
        return TestHelper.createExecutableApp(app, configuration, this.stateDir);
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexApplication() {
        this.newTestClient().createTopic(ComplexTopologyApplication.THROUGH_TOPIC);
        return new ConfiguredStreamsApp<>(new ComplexTopologyApplication(),
                new StreamsAppConfiguration(StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .outputTopic("output")
                        .build()));
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexCleanUpHookApplication() {
        this.newTestClient().createTopic(ComplexTopologyApplication.THROUGH_TOPIC);
        return new ConfiguredStreamsApp<>(new ComplexTopologyApplication() {
            @Override
            public StreamsCleanUpConfiguration setupCleanUp(
                    final AppConfiguration<StreamsTopicConfig> configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(StreamsCleanUpRunnerTest.this.topicHook);
            }
        }, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, Long>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new LongDeserializer())
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(TestHelper::toKeyValue)
                .toList();
    }

    private void assertContent(final String outputTopic,
            final Iterable<? extends KeyValue<String, Long>> expectedValues, final String description) {
        final List<KeyValue<String, Long>> output = this.readOutputTopic(outputTopic);
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void assertSize(final String outputTopic, final int expectedMessageCount) {
        final List<KeyValue<String, Long>> records = this.readOutputTopic(outputTopic);
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }

}
