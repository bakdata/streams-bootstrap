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


import static com.bakdata.kafka.integration.StreamsRunnerTest.configureApp;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.ConfiguredStreamsApp;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.ExecutableStreamsApp;
import com.bakdata.kafka.HasTopicHooks.TopicHook;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsCleanUpConfiguration;
import com.bakdata.kafka.StreamsCleanUpRunner;
import com.bakdata.kafka.StreamsRunner;
import com.bakdata.kafka.StreamsSetupConfiguration;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import com.bakdata.kafka.test_applications.MirrorKeyWithAvro;
import com.bakdata.kafka.test_applications.MirrorValueWithAvro;
import com.bakdata.kafka.test_applications.WordCount;
import com.bakdata.kafka.test_applications.WordCountPattern;
import com.bakdata.kafka.util.ConsumerGroupClient;
import com.bakdata.kafka.util.ImprovedAdminClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
class StreamsCleanUpRunnerTest extends KafkaTest {
    private static final int TIMEOUT_SECONDS = 10;
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private TopicHook topicHook;

    private static ConfiguredStreamsApp<StreamsApp> createWordCountPatternApplication() {
        return configureApp(new WordCountPattern(), StreamsTopicConfig.builder()
                .inputPattern(Pattern.compile(".*_topic"))
                .outputTopic("word_output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createWordCountApplication() {
        return configureApp(new WordCount(), StreamsTopicConfig.builder()
                .inputTopics(List.of("word_input"))
                .outputTopic("word_output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorValueApplication() {
        return configureApp(new MirrorValueWithAvro(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorKeyApplication() {
        return configureApp(new MirrorKeyWithAvro(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private static void reset(final ExecutableApp<?, StreamsCleanUpRunner, ?> app) {
        app.createCleanUpRunner().reset();
    }

    private static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        app.createCleanUpRunner().clean();
    }

    private static void run(final ExecutableApp<StreamsRunner, ?, ?> app) throws InterruptedException {
        try (final StreamsRunner runner = app.createRunner()) {
            StreamsRunnerTest.run(runner);
            // Wait until stream application has consumed all data
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0), List.of("blub", "bla", "blub"))
                    .useDefaults();
            this.kafkaCluster.send(sendRequest);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            clean(executableApp);

            this.softly.assertThat(this.kafkaCluster.exists(app.getTopics().getOutputTopic()))
                    .as("Output topic is deleted")
                    .isFalse();
        }
    }

    @Test
    void shouldDeleteConsumerGroup() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0), List.of("blub", "bla", "blub"))
                    .useDefaults();
            this.kafkaCluster.send(sendRequest);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final ConsumerGroupClient adminClient = this.createAdminClient().getConsumerGroupClient()) {
                this.softly.assertThat(adminClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            clean(executableApp);

            try (final ConsumerGroupClient adminClient = this.createAdminClient().getConsumerGroupClient()) {
                this.softly.assertThat(adminClient.exists(app.getUniqueAppId()))
                        .as("Consumer group is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldNotThrowAnErrorIfConsumerGroupDoesNotExist() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0), List.of("blub", "bla", "blub"))
                    .useDefaults();
            this.kafkaCluster.send(sendRequest);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final ConsumerGroupClient adminClient = this.createAdminClient().getConsumerGroupClient()) {
                this.softly.assertThat(adminClient.exists(app.getUniqueAppId()))
                        .as("Consumer group exists")
                        .isTrue();
            }

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            try (final ConsumerGroupClient adminClient = this.createAdminClient().getConsumerGroupClient()) {
                adminClient.deleteConsumerGroup(app.getUniqueAppId());
                this.softly.assertThat(adminClient.exists(app.getUniqueAppId()))
                        .as("Consumer group is deleted")
                        .isFalse();
            }
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldDeleteInternalTopics() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {

            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0),
                            Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String uniqueAppId = app.getUniqueAppId();
            final String internalTopic =
                    uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition";
            final String backingTopic =
                    uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";
            final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

            for (final String inputTopic : inputTopics) {
                this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
            }
            this.softly.assertThat(this.kafkaCluster.exists(internalTopic)).isTrue();
            this.softly.assertThat(this.kafkaCluster.exists(backingTopic)).isTrue();
            this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reset(executableApp);

            for (final String inputTopic : inputTopics) {
                this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
            }
            this.softly.assertThat(this.kafkaCluster.exists(internalTopic)).isFalse();
            this.softly.assertThat(this.kafkaCluster.exists(backingTopic)).isFalse();
            this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();
        }
    }

    @Test
    void shouldDeleteIntermediateTopics() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {

            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0),
                            Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            final List<String> inputTopics = app.getTopics().getInputTopics();
            final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

            for (final String inputTopic : inputTopics) {
                this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
            }
            this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            clean(executableApp);

            for (final String inputTopic : inputTopics) {
                this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
            }
            this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isFalse();
        }
    }

    @Test
    void shouldDeleteState() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0), List.of("blub", "bla", "blub"))
                    .useDefaults();
            this.kafkaCluster.send(sendRequest);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "All entries are once in the input topic after the 1st run");

            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reset(executableApp);

            run(executableApp);
            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .collect(Collectors.toList());
            this.assertContent(app.getTopics().getOutputTopic(), entriesTwice,
                    "All entries are twice in the input topic after the 2nd run");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecords() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                    .inTransaction(app.getTopics().getInputTopics().get(0), List.of("a", "b", "c"))
                    .useDefaults();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    @Test
    void shouldDeleteValueSchema()
            throws InterruptedException, IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorValueApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                    .inTransaction(inputTopic, Collections.singletonList(testRecord))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            throws InterruptedException, IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final SendKeyValuesTransactional<TestRecord, String> sendRequest = SendKeyValuesTransactional
                    .inTransaction(inputTopic, Collections.singletonList(new KeyValue<>(testRecord, "val")))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            throws InterruptedException, IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                    .inTransaction(inputTopic, Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            throws InterruptedException, IOException, RestClientException {
        try (final ConfiguredStreamsApp<StreamsApp> app = this.createComplexApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint());
                final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient()) {
            final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                    .inTransaction(inputTopic, Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                    .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryMockExtension.getUrl())
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                    .build();
            this.kafkaCluster.send(sendRequest);

            run(executableApp);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
            verifyNoMoreInteractions(this.topicHook);
        }
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopic() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(this.createEndpoint())) {
            // if we don't run the app, the coordinator will be unavailable
            run(executableApp);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldThrowExceptionOnResetterError() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorKeyApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpoint());
                final StreamsRunner runner = executableApp.createRunner()) {
            this.kafkaCluster.createTopic(TopicConfig.withName(app.getTopics().getInputTopics().get(0)).useDefaults());
            StreamsRunnerTest.run(runner);
            // Wait until stream application has consumed all data
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            // should throw exception because consumer group is still active
            this.softly.assertThatThrownBy(() -> reset(executableApp))
                    .isInstanceOf(CleanUpException.class)
                    .hasMessageContaining("Error running streams resetter. Exit code 1");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithPattern() throws InterruptedException {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountPatternApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = app.withEndpoint(
                        this.createEndpointWithoutSchemaRegistry())) {
            this.kafkaCluster.send(SendValuesTransactional.inTransaction("input_topic",
                    Arrays.asList("a", "b")).useDefaults());
            this.kafkaCluster.send(SendValuesTransactional.inTransaction("another_topic",
                    List.of("c")).useDefaults());

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexApplication() {
        this.kafkaCluster.createTopic(TopicConfig.withName(ComplexTopologyApplication.THROUGH_TOPIC).useDefaults());
        return configureApp(new ComplexTopologyApplication(), StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private ConfiguredStreamsApp<StreamsApp> createComplexCleanUpHookApplication() {
        this.kafkaCluster.createTopic(TopicConfig.withName(ComplexTopologyApplication.THROUGH_TOPIC).useDefaults());
        return configureApp(new ComplexTopologyApplication() {
            @Override
            public StreamsCleanUpConfiguration setupCleanUp(final StreamsSetupConfiguration configuration) {
                return super.setupCleanUp(configuration)
                        .registerTopicHook(StreamsCleanUpRunnerTest.this.topicHook);
            }
        }, StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build());
    }

    private ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.create(this.createEndpoint().createKafkaProperties());
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) throws InterruptedException {
        final ReadKeyValues<String, Long> readRequest = ReadKeyValues.from(outputTopic, Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class).build();
        return this.kafkaCluster.read(readRequest);
    }

    private void assertContent(final String outputTopic,
            final Iterable<? extends KeyValue<String, Long>> expectedValues, final String description)
            throws InterruptedException {
        final List<KeyValue<String, Long>> output = this.readOutputTopic(outputTopic);
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void assertSize(final String outputTopic, final int expectedMessageCount) throws InterruptedException {
        final List<KeyValue<String, Long>> records = this.readOutputTopic(outputTopic);
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }

}
