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


import static com.bakdata.kafka.AsyncRunnable.runAsync;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ConfiguredStreamsApp;
import com.bakdata.kafka.EffectiveAppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.ExecutableStreamsApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsCleanUpConfiguration;
import com.bakdata.kafka.StreamsCleanUpRunner;
import com.bakdata.kafka.StreamsRunner;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import com.bakdata.kafka.test_applications.MirrorKeyWithAvro;
import com.bakdata.kafka.test_applications.MirrorValueWithAvro;
import com.bakdata.kafka.test_applications.WordCount;
import com.bakdata.kafka.test_applications.WordCountPattern;
import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.TopicClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    static <K, V> KeyValue<K, V> toKeyValue(final ConsumerRecord<K, V> consumerRecord) {
        return new KeyValue<>(consumerRecord.key(), consumerRecord.value());
    }

    private static void reset(final ExecutableApp<?, StreamsCleanUpRunner, ?> app) {
        try (final StreamsCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        try (final CleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.clean();
        }
    }

    ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app, final StreamsTopicConfig topics) {
        return StreamsRunnerTest.configureApp(app, topics, this.stateDir);
    }

    @Test
    void shouldDeleteTopic() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
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

            this.run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            this.awaitClosed(executableApp);
            clean(executableApp);

            try (final ImprovedAdminClient admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                this.softly.assertThat(topicClient.exists(app.getTopics().getOutputTopic()))
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldDeleteConsumerGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
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

            this.run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            this.awaitClosed(executableApp);
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
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
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

            this.run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final ImprovedAdminClient adminClient = testClient.admin();
                    final ConsumerGroupClient consumerGroupClient = adminClient.getConsumerGroupClient()) {
                this.softly.assertThat(consumerGroupClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            this.awaitClosed(executableApp);

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
    void shouldDeleteInternalTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {

            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            this.run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String uniqueAppId = app.getUniqueAppId();
            final String internalTopic =
                    uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition";
            final String backingTopic =
                    uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";
            final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

            try (final ImprovedAdminClient admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(internalTopic)).isTrue();
                this.softly.assertThat(topicClient.exists(backingTopic)).isTrue();
                this.softly.assertThat(topicClient.exists(manualTopic)).isTrue();
            }

            this.awaitClosed(executableApp);
            reset(executableApp);

            try (final ImprovedAdminClient admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(internalTopic)).isFalse();
                this.softly.assertThat(topicClient.exists(backingTopic)).isFalse();
                this.softly.assertThat(topicClient.exists(manualTopic)).isTrue();
            }
        }
    }

    @Test
    void shouldDeleteIntermediateTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {

            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            this.run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

            try (final ImprovedAdminClient admin = testClient.admin();
                    final TopicClient topicClient = admin.getTopicClient()) {
                for (final String inputTopic : inputTopics) {
                    this.softly.assertThat(topicClient.exists(inputTopic)).isTrue();
                }
                this.softly.assertThat(topicClient.exists(manualTopic)).isTrue();
            }

            this.awaitClosed(executableApp);
            clean(executableApp);

            try (final ImprovedAdminClient admin = testClient.admin();
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
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
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

            this.run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "All entries are once in the input topic after the 1st run");

            this.awaitClosed(executableApp);
            reset(executableApp);

            this.run(executableApp);
            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .collect(Collectors.toList());
            this.assertContent(app.getTopics().getOutputTopic(), entriesTwice,
                    "All entries are twice in the input topic after the 2nd run");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecords() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b"),
                            new SimpleProducerRecord<>(null, "c")
                    ));

            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all stream applications are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
            reset(executableApp);

            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    @Test
    void shouldDeleteValueSchema()
            throws IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorValueApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, testRecord)
                    ));

            this.run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
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
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(testRecord, "val")
                    ));

            this.run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
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
    void shouldDeleteSchemaOfInternalTopics()
            throws IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            this.run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
            final String inputSubject = inputTopic + "-value";
            final String uniqueAppId = app.getUniqueAppId();
            final String internalSubject =
                    uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition" + "-value";
            final String backingSubject =
                    uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog" + "-value";
            final String manualSubject = ComplexTopologyApplication.THROUGH_TOPIC + "-value";
            this.softly.assertThat(client.getAllSubjects())
                    .contains(inputSubject, internalSubject, backingSubject, manualSubject);
            reset(executableApp);

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(internalSubject, backingSubject)
                    .contains(inputSubject, manualSubject);
        }
    }

    @Test
    void shouldDeleteSchemaOfIntermediateTopics()
            throws IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("key 1", testRecord)
                    ));

            this.run(executableApp);

            // Wait until all stream applications are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
            final String inputSubject = inputTopic + "-value";
            final String manualSubject = ComplexTopologyApplication.THROUGH_TOPIC + "-value";
            this.softly.assertThat(client.getAllSubjects())
                    .contains(inputSubject, manualSubject);
            clean(executableApp);

            this.softly.assertThat(client.getAllSubjects())
                    .doesNotContain(manualSubject)
                    .contains(inputSubject);
        }
    }

    @Test
    void shouldCallCleanupHookForInternalTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexCleanUpHookApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {
            reset(executableApp);
            final String uniqueAppId = app.getUniqueAppId();
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-changelog");
            verify(this.topicHook).deleted(uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog");
            verify(this.topicHook).close();
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexCleanUpHookApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {
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
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldThrowExceptionOnResetterError() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final StreamsRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            runAsync(runner);
            // Wait until stream application has consumed all data
            this.awaitActive(executableApp);
            // should throw exception because consumer group is still active
            this.softly.assertThatThrownBy(() -> reset(executableApp))
                    .isInstanceOf(CleanUpException.class)
                    .hasMessageContaining("Error running streams resetter. Exit code 1");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithPattern() {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createWordCountPatternApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to("input_topic", List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b")
                    ));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to("another_topic", List.of(
                            new SimpleProducerRecord<>(null, "c")
                    ));

            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all streams application are completely stopped before triggering cleanup
            this.awaitClosed(executableApp);
            reset(executableApp);

            this.run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    private ConfiguredStreamsApp<StreamsApp> createWordCountPatternApplication() {
        return this.configureApp(new WordCountPattern(), StreamsTopicConfig.builder()
                .inputPattern(Pattern.compile(".*_topic"))
                .outputTopic("word_output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createWordCountApplication() {
        return this.configureApp(new WordCount(), StreamsTopicConfig.builder()
                .inputTopics(List.of("word_input"))
                .outputTopic("word_output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createMirrorValueApplication() {
        return this.configureApp(new MirrorValueWithAvro(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createMirrorKeyApplication() {
        return this.configureApp(new MirrorKeyWithAvro(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private void run(final ExecutableStreamsApp<?> app) {
        try (final StreamsRunner runner = app.createRunner()) {
            runAsync(runner);
            // Wait until stream application has consumed all data
            this.awaitProcessing(app);
        }
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexApplication() {
        this.newTestClient().createTopic(ComplexTopologyApplication.THROUGH_TOPIC);
        return this.configureApp(new ComplexTopologyApplication(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexCleanUpHookApplication() {
        this.newTestClient().createTopic(ComplexTopologyApplication.THROUGH_TOPIC);
        return this.configureApp(new ComplexTopologyApplication() {
            @Override
            public StreamsCleanUpConfiguration setupCleanUp(
                    final EffectiveAppConfiguration<StreamsTopicConfig> configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(StreamsCleanUpRunnerTest.this.topicHook);
            }
        }, StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, Long>> records = this.newTestClient().read()
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(StreamsCleanUpRunnerTest::toKeyValue)
                .collect(Collectors.toList());
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
