/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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


import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.CloseFlagApp;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import com.bakdata.kafka.test_applications.MirrorKeyWithAvro;
import com.bakdata.kafka.test_applications.MirrorValueWithAvro;
import com.bakdata.kafka.test_applications.WordCount;
import com.bakdata.kafka.test_applications.WordCountPattern;
import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsCleanUpTest {
    private static final int TIMEOUT_SECONDS = 10;
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private EmbeddedKafkaCluster kafkaCluster;
    private KafkaStreamsApplication app = null;
    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private Consumer<String> topicCleanUpHook;

    @BeforeEach
    void setup() throws InterruptedException {
        this.kafkaCluster = provisionWith(defaultClusterConfig());
        this.kafkaCluster.start();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (this.app != null) {
            this.app.close();
            this.app.getStreams().cleanUp();
            this.app = null;
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.kafkaCluster.stop();
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        this.app = this.createWordCountApplication();
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final List<KeyValue<String, Long>> expectedValues =
                List.of(new KeyValue<>("blub", 1L),
                        new KeyValue<>("bla", 1L),
                        new KeyValue<>("blub", 2L)
                );

        this.runAndAssertContent(expectedValues, "WordCount contains all elements after first run");

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUpWithDeletion();

        this.softly.assertThat(this.kafkaCluster.exists(this.app.getOutputTopic()))
                .as("Output topic is deleted")
                .isFalse();

        this.softly.assertThat(this.kafkaCluster.exists(this.app.getErrorTopic()))
                .as("Error topic is deleted")
                .isFalse();
    }

    @Test
    void shouldDeleteConsumerGroup() throws InterruptedException {
        this.app = this.createWordCountApplication();
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final List<KeyValue<String, Long>> expectedValues =
                List.of(new KeyValue<>("blub", 1L),
                        new KeyValue<>("bla", 1L),
                        new KeyValue<>("blub", 2L)
                );

        this.runAndAssertContent(expectedValues, "WordCount contains all elements after first run");

        try (final AdminClient adminClient = AdminClient.create(this.app.getKafkaProperties())) {
            this.softly.assertThat(adminClient.listConsumerGroups().all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .extracting(ConsumerGroupListing::groupId)
                    .as("Consumer group exists")
                    .contains(this.app.getUniqueAppId());
        } catch (final TimeoutException | ExecutionException e) {
            throw new RuntimeException("Error retrieving consumer groups", e);
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUpWithDeletion();

        try (final AdminClient adminClient = AdminClient.create(this.app.getKafkaProperties())) {
            this.softly.assertThat(adminClient.listConsumerGroups().all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .extracting(ConsumerGroupListing::groupId)
                    .as("Consumer group is deleted")
                    .doesNotContain(this.app.getUniqueAppId());
        } catch (final TimeoutException | ExecutionException e) {
            throw new RuntimeException("Error retrieving consumer groups", e);
        }
    }

    @Test
    void shouldNotThrowAnErrorIfConsumerGroupDoesNotExist() throws InterruptedException {
        this.app = this.createWordCountApplication();
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final List<KeyValue<String, Long>> expectedValues =
                List.of(new KeyValue<>("blub", 1L),
                        new KeyValue<>("bla", 1L),
                        new KeyValue<>("blub", 2L)
                );

        this.runAndAssertContent(expectedValues, "WordCount contains all elements after first run");

        try (final AdminClient adminClient = AdminClient.create(this.app.getKafkaProperties())) {
            this.softly.assertThat(adminClient.listConsumerGroups().all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .extracting(ConsumerGroupListing::groupId)
                    .as("Consumer group exists")
                    .contains(this.app.getUniqueAppId());
        } catch (final TimeoutException | ExecutionException e) {
            throw new RuntimeException("Error retrieving consumer groups", e);
        }

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        try (final AdminClient adminClient = AdminClient.create(this.app.getKafkaProperties())) {
            adminClient.deleteConsumerGroups(List.of(this.app.getUniqueAppId())).all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(adminClient.listConsumerGroups().all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .extracting(ConsumerGroupListing::groupId)
                    .as("Consumer group is deleted")
                    .doesNotContain(this.app.getUniqueAppId());
        } catch (final TimeoutException | ExecutionException e) {
            throw new RuntimeException("Error deleting consumer group", e);
        }
        this.softly.assertThatCode(this::runCleanUpWithDeletion).doesNotThrowAnyException();
    }

    @Test
    void shouldDeleteInternalTopics() throws InterruptedException {
        this.app = this.createComplexApplication();

        final String inputTopic = this.app.getInputTopic();
        final String internalTopic =
                this.app.getUniqueAppId() + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition";
        final String backingTopic =
                this.app.getUniqueAppId() + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog";
        final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();

        this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
        this.softly.assertThat(this.kafkaCluster.exists(internalTopic)).isTrue();
        this.softly.assertThat(this.kafkaCluster.exists(backingTopic)).isTrue();
        this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUp();

        this.softly.assertThat(this.kafkaCluster.exists(inputTopic)).isTrue();
        this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();

        this.softly.assertThat(this.kafkaCluster.exists(internalTopic)).isFalse();
        this.softly.assertThat(this.kafkaCluster.exists(backingTopic)).isFalse();
    }


    @Test
    void shouldDeleteIntermediateTopics() throws InterruptedException {
        this.app = this.createComplexApplication();

        final String manualTopic = ComplexTopologyApplication.THROUGH_TOPIC;

        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();

        this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isTrue();

        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUpWithDeletion();

        this.softly.assertThat(this.kafkaCluster.exists(manualTopic)).isFalse();
    }

    @Test
    void shouldDeleteState() throws InterruptedException {
        this.app = this.createWordCountApplication();
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final List<KeyValue<String, Long>> expectedValues = List.of(
                new KeyValue<>("blub", 1L),
                new KeyValue<>("bla", 1L),
                new KeyValue<>("blub", 2L)
        );

        this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run");
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUp();

        final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                .flatMap(entry -> Stream.of(entry, entry))
                .collect(Collectors.toList());
        this.runAndAssertContent(entriesTwice, "All entries are twice in the input topic after the 2nd run");
    }

    @Test
    void shouldReprocessAlreadySeenRecords() throws InterruptedException {
        this.app = this.createWordCountApplication();
        final SendValuesTransactional<String> sendRequest =
                SendValuesTransactional.inTransaction(this.app.getInputTopic(),
                        Arrays.asList("a", "b", "c")).useDefaults();
        this.kafkaCluster.send(sendRequest);

        this.runAndAssertSize(3);
        this.runAndAssertSize(3);

        // Wait until all stream application are completely stopped before triggering cleanup
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUp();
        this.runAndAssertSize(6);
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithPattern() throws InterruptedException {
        this.app = this.createWordCountPatternApplication();
        this.kafkaCluster.send(SendValuesTransactional.inTransaction("input_topic",
                Arrays.asList("a", "b")).useDefaults());
        this.kafkaCluster.send(SendValuesTransactional.inTransaction("another_topic",
                List.of("c")).useDefaults());

        this.runAndAssertSize(3);
        this.runAndAssertSize(3);

        // Wait until all stream application are completely stopped before triggering cleanup
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUp();
        this.runAndAssertSize(6);
    }

    @Test
    void shouldDeleteValueSchema()
            throws InterruptedException, IOException, RestClientException {
        this.app = this.createMirrorValueApplication();
        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(testRecord))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThat(client.getAllSubjects())
                .contains(this.app.getOutputTopic() + "-value", this.app.getInputTopic() + "-value");
        this.runCleanUpWithDeletion();
        this.softly.assertThat(client.getAllSubjects())
                .doesNotContain(this.app.getOutputTopic() + "-value")
                .contains(this.app.getInputTopic() + "-value");
    }

    @Test
    void shouldDeleteKeySchema()
            throws InterruptedException, IOException, RestClientException {
        this.app = this.createMirrorKeyApplication();
        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendKeyValuesTransactional<TestRecord, String> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>(testRecord, "val")))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThat(client.getAllSubjects())
                .contains(this.app.getOutputTopic() + "-key", this.app.getInputTopic() + "-key");
        this.runCleanUpWithDeletion();
        this.softly.assertThat(client.getAllSubjects())
                .doesNotContain(this.app.getOutputTopic() + "-key")
                .contains(this.app.getInputTopic() + "-key");
    }

    @Test
    void shouldDeleteSchemaOfInternalTopics()
            throws InterruptedException, IOException, RestClientException {
        this.app = this.createComplexApplication();

        final String inputSubject = this.app.getInputTopic() + "-value";
        final String internalSubject =
                this.app.getUniqueAppId() + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition" + "-value";
        final String backingSubject =
                this.app.getUniqueAppId() + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog" + "-value";
        final String manualSubject = ComplexTopologyApplication.THROUGH_TOPIC + "-value";

        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThat(client.getAllSubjects())
                .contains(inputSubject, internalSubject, backingSubject, manualSubject);

        this.runCleanUp();

        this.softly.assertThat(client.getAllSubjects())
                .doesNotContain(internalSubject, backingSubject)
                .contains(inputSubject, manualSubject);
    }


    @Test
    void shouldDeleteSchemaOfIntermediateTopics()
            throws InterruptedException, IOException, RestClientException {
        this.app = this.createComplexApplication();

        final String manualSubject = ComplexTopologyApplication.THROUGH_TOPIC + "-value";

        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();
        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();
        final SendKeyValuesTransactional<String, TestRecord> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>("key 1", testRecord)))
                .with(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.runAppAndClose();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThat(client.getAllSubjects()).contains(manualSubject);
        this.runCleanUpWithDeletion();
        this.softly.assertThat(client.getAllSubjects()).doesNotContain(manualSubject);
    }

    @Test
    void shouldCallCleanupHookForInternalTopics() {
        this.app = this.createComplexCleanUpHookApplication();

        this.runCleanUp();
        final String uniqueAppId = this.app.getUniqueAppId();
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition");
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-changelog");
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog");
        verifyNoMoreInteractions(this.topicCleanUpHook);
    }

    @Test
    void shouldCallCleanUpHookForAllTopics() {
        this.app = this.createComplexCleanUpHookApplication();

        this.runCleanUpWithDeletion();
        final String uniqueAppId = this.app.getUniqueAppId();
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-repartition");
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000008-changelog");
        verify(this.topicCleanUpHook).accept(uniqueAppId + "-KSTREAM-REDUCE-STATE-STORE-0000000003-changelog");
        verify(this.topicCleanUpHook).accept(ComplexTopologyApplication.THROUGH_TOPIC);
        verify(this.topicCleanUpHook).accept(this.app.getOutputTopic());
        verifyNoMoreInteractions(this.topicCleanUpHook);
    }

    @Test
    void shouldCallClose() throws InterruptedException {
        final CloseFlagApp closeApplication = this.createCloseApplication();
        this.app = closeApplication;
        this.kafkaCluster.createTopic(TopicConfig.withName(this.app.getInputTopic()).useDefaults());
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        // if we don't run the app, the coordinator will be unavailable
        this.runAppAndClose();
        closeApplication.setClosed(false);
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.runCleanUpWithDeletion();
        this.softly.assertThat(closeApplication.isClosed()).isTrue();
    }

    @Test
    @SetEnvironmentVariable(key = "STREAMS_FOO_BAR", value = "baz")
    void shouldConfigureAdminClient() {
        final CloseFlagApp closeApplication = this.createCloseApplication();
        final ImprovedAdminClient adminClient = closeApplication.createAdminClient();
        final Properties properties = adminClient.getProperties();
        this.softly.assertThat(properties.getProperty("foo.bar")).isEqualTo("baz");
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopic() throws InterruptedException {
        this.app = this.createMirrorKeyApplication();
        // if we don't run the app, the coordinator will be unavailable
        this.runAppAndClose();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.softly.assertThatCode(this::runCleanUpWithDeletion).doesNotThrowAnyException();
    }

    @Test
    void shouldThrowExceptionOnResetterError() throws InterruptedException {
        this.app = this.createMirrorKeyApplication();
        this.kafkaCluster.createTopic(TopicConfig.withName(this.app.getInputTopic()).useDefaults());
        this.runApp();
        //should throw exception because consumer group is still active
        this.softly.assertThatThrownBy(this::runCleanUpWithDeletion)
                .isInstanceOf(CleanUpException.class)
                .hasMessageContaining("Error running streams resetter. Exit code 1");
        this.app.close();
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) throws InterruptedException {
        final ReadKeyValues<String, Long> readRequest = ReadKeyValues.from(outputTopic, Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class).build();
        return this.kafkaCluster.read(readRequest);
    }

    private void runCleanUp() {
        this.app.setCleanUp(true);
        this.app.run();
        this.app.setCleanUp(false);
    }

    private void runCleanUpWithDeletion() {
        this.app.setDeleteOutputTopic(true);
        this.runCleanUp();
        this.app.setDeleteOutputTopic(false);
    }

    private void runAndAssertContent(final Iterable<? extends KeyValue<String, Long>> expectedValues,
            final String description)
            throws InterruptedException {
        this.runAppAndClose();

        final List<KeyValue<String, Long>> output = this.readOutputTopic(this.app.getOutputTopic());
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void runAndAssertSize(final int expectedMessageCount)
            throws InterruptedException {
        this.runAppAndClose();
        final List<KeyValue<String, Long>> records = this.readOutputTopic(this.app.getOutputTopic());
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }

    private void runApp() throws InterruptedException {
        // run in Thread because the application blocks indefinitely
        new Thread(this.app).start();
        // Wait until stream application has consumed all data
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void runAppAndClose() throws InterruptedException {
        this.runApp();
        this.app.close();
    }

    private KafkaStreamsApplication createWordCountApplication() {
        return this.setupAppNoSr(new WordCount(), "word_input", "word_output", "word_error");
    }

    private KafkaStreamsApplication createWordCountPatternApplication() {
        return this.setupAppNoSr(new WordCountPattern(), Pattern.compile(".*_topic"), "word_output", "word_error");
    }

    private KafkaStreamsApplication createMirrorValueApplication() {
        return this.setupAppWithSr(new MirrorValueWithAvro(), "input", "output", "key_error");
    }

    private CloseFlagApp createCloseApplication() {
        return this.setupAppWithSr(new CloseFlagApp(), "input", "output", "key_error");
    }

    private KafkaStreamsApplication createMirrorKeyApplication() {
        return this.setupAppWithSr(new MirrorKeyWithAvro(), "input", "output", "value_error");
    }

    private KafkaStreamsApplication createComplexApplication() {
        this.kafkaCluster.createTopic(TopicConfig.withName(ComplexTopologyApplication.THROUGH_TOPIC).useDefaults());
        return this.setupAppWithSr(new ComplexTopologyApplication(), "input", "output", "value_error");
    }

    private KafkaStreamsApplication createComplexCleanUpHookApplication() {
        this.kafkaCluster.createTopic(TopicConfig.withName(ComplexTopologyApplication.THROUGH_TOPIC).useDefaults());
        return this.setupAppWithSr(new ComplexTopologyApplication() {
            @Override
            protected void cleanUpRun(final CleanUpRunner cleanUpRunner) {
                cleanUpRunner.registerTopicCleanUpHook(StreamsCleanUpTest.this.topicCleanUpHook);
                super.cleanUpRun(cleanUpRunner);
            }
        }, "input", "output", "value_error");
    }

    private <T extends KafkaStreamsApplication> T setupAppWithSr(final T application, final String inputTopicName,
            final String outputTopicName, final String errorTopicName) {
        this.setupApp(application, outputTopicName, errorTopicName);
        application.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        application.setInputTopics(List.of(inputTopicName));
        return application;
    }

    private <T extends KafkaStreamsApplication> T setupAppNoSr(final T application, final String inputTopicName,
            final String outputTopicName, final String errorTopicName) {
        this.setupApp(application, outputTopicName, errorTopicName);
        application.setInputTopics(List.of(inputTopicName));
        return application;
    }

    private <T extends KafkaStreamsApplication> T setupAppNoSr(final T application, final Pattern inputPattern,
            final String outputTopicName, final String errorTopicName) {
        this.setupApp(application, outputTopicName, errorTopicName);
        application.setInputPattern(inputPattern);
        return application;
    }

    private <T extends KafkaStreamsApplication> void setupApp(final T application, final String outputTopicName,
            final String errorTopicName) {
        application.setOutputTopic(outputTopicName);
        application.setErrorTopic(errorTopicName);
        application.setBrokers(this.kafkaCluster.getBrokerList());
        application.setProductive(false);
        application.setStreamsConfig(Map.of(
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
        ));
    }
}
