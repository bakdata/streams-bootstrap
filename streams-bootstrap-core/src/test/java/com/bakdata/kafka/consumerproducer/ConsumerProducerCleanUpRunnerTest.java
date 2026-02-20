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

package com.bakdata.kafka.consumerproducer;


import static com.bakdata.kafka.consumerproducer.TestHelper.run;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient;
import com.bakdata.kafka.admin.ConsumerGroupsClient.ConsumerGroupClient;
import com.bakdata.kafka.admin.TopicsClient;
import com.bakdata.kafka.consumerproducer.apps.MirrorKeyWithAvroConsumerProducer;
import com.bakdata.kafka.consumerproducer.apps.MirrorValueWithAvroConsumerProducer;
import com.bakdata.kafka.consumerproducer.apps.StringConsumerProducer;
import com.bakdata.kafka.consumerproducer.apps.StringPatternConsumerProducer;
import com.bakdata.kafka.streams.StreamsCleanUpConfiguration;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ConsumerProducerCleanUpRunnerTest extends KafkaTest {
    private static final ConsumerProducerTopicConfig TOPIC_CONFIG = ConsumerProducerTopicConfig.builder()
            .inputTopics(List.of("input"))
            .outputTopic("output")
            .errorTopic("error")
            .build();
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private TopicHook topicHook;

    private static void reset(final ExecutableApp<?, ConsumerProducerCleanUpRunner, ?> app) {
        try (final ConsumerProducerCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        try (final CleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.clean();
        }
    }

    static ConfiguredConsumerProducerApp<ConsumerProducerApp> createStringConsumerProducer() {
        return new ConfiguredConsumerProducerApp<>(new StringConsumerProducer(),
                new ConsumerProducerAppConfiguration(TOPIC_CONFIG));
    }

    static ConfiguredConsumerProducerApp<ConsumerProducerApp> createMirrorKeyConsumerProducer() {
        return new ConfiguredConsumerProducerApp<>(new MirrorKeyWithAvroConsumerProducer(),
                new ConsumerProducerAppConfiguration(TOPIC_CONFIG));
    }

    static ConfiguredConsumerProducerApp<ConsumerProducerApp> createMirrorValueConsumerProducer() {
        return new ConfiguredConsumerProducerApp<>(new MirrorValueWithAvroConsumerProducer(),
                new ConsumerProducerAppConfiguration(TOPIC_CONFIG));
    }

    static ConfiguredConsumerProducerApp<ConsumerProducerApp> createStringPatternConsumerProducer() {
        final ConsumerProducerTopicConfig topics = ConsumerProducerTopicConfig.builder()
                .inputPattern(Pattern.compile(".*_topic"))
                .outputTopic("output")
                .build();
        return new ConfiguredConsumerProducerApp<>(new StringPatternConsumerProducer(),
                new ConsumerProducerAppConfiguration(topics));
    }

    static ExecutableConsumerProducerApp<ConsumerProducerApp> createExecutableApp(
            final ConfiguredConsumerProducerApp<ConsumerProducerApp> app,
            final RuntimeConfiguration runtimeConfiguration) {
        return app.withRuntimeConfiguration(runtimeConfiguration);
    }

    private ConfiguredConsumerProducerApp<ConsumerProducerApp> createCleanUpHookApplication() {
        return new ConfiguredConsumerProducerApp<>(new StringConsumerProducer() {
            @Override
            public StreamsCleanUpConfiguration setupCleanUp(
                    final AppConfiguration<ConsumerProducerTopicConfig> configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(ConsumerProducerCleanUpRunnerTest.this.topicHook);
            }
        }, new ConsumerProducerAppConfiguration(TOPIC_CONFIG));
    }

    @Test
    void shouldDeleteTopic() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.createTopic(app.getTopics().getOutputTopic());
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

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "Output contains all elements after first run");

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX admin = testClient.admin()) {
                final TopicsClient topicClient = admin.topics();
                this.softly.assertThat(topicClient.topic(app.getTopics().getOutputTopic()).exists())
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldDeleteErrorTopic() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.createTopic(app.getTopics().getErrorTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            run(executableApp);
            this.assertContent(app.getTopics().getErrorTopic(), List.of(),
                    "Error topic exists and is empty");

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX admin = testClient.admin()) {
                final TopicsClient topicClient = admin.topics();
                this.softly.assertThat(topicClient.topic(app.getTopics().getErrorTopic()).exists())
                        .as("Error topic is deleted")
                        .isFalse();
            }
        }
    }


    @Test
    void shouldDeleteConsumerGroup() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
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

            final StringConsumerProducer stringConsumer = (StringConsumerProducer) app.app();

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "Output contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueAppId());
                this.softly.assertThat(consumerGroupClient.exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

            stringConsumer.close();
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
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
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

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "Contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupsClient groups = adminClient.consumerGroups();
                this.softly.assertThat(groups.group(app.getUniqueAppId()).exists())
                        .as("Consumer group exists")
                        .isTrue();
            }

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
    void shouldReprocessAlreadySeenRecordsWithPattern() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringPatternConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            final String inputTopic = "input_topic";
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(inputTopic, List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b"),
                            new SimpleProducerRecord<>(null, "c")
                    ));

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    @Test
    void shouldDeleteValueSchema()
            throws IOException, RestClientException {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createMirrorValueConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfigWithSchemaRegistry());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new SpecificAvroSerializer<>())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, testRecord)
                    ));
            run(executableApp);

            // Wait until all applications are completely stopped before triggering cleanup
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
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createMirrorKeyConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfigWithSchemaRegistry());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new SpecificAvroSerializer<>())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(testRecord, "val")
                    ));

            run(executableApp);

            // Wait until all applications are completely stopped before triggering cleanup
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

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = this.createCleanUpHookApplication();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            clean(executableApp);
            verify(this.topicHook).deleted(app.getTopics().getOutputTopic());
            verify(this.topicHook).close();
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopic() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldThrowExceptionOnResetterError() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig());
                final ConsumerProducerRunner runner = executableApp.createRunner()) {
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
    void shouldReprocessAlreadySeenRecords() {
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ExecutableConsumerProducerApp<ConsumerProducerApp> executableApp = createExecutableApp(app,
                        this.createConfig())) {
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

            // Wait until all applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
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

    private void assertContent(final String outputTopic,
            final Iterable<? extends KeyValue<String, String>> expectedValues, final String description) {
        final List<KeyValue<String, String>> output = this.readOutputTopic(outputTopic);
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void assertSize(final String outputTopic, final int expectedMessageCount) {
        final List<KeyValue<String, String>> records = this.readOutputTopic(outputTopic);
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }

}
