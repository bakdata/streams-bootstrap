/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.common_kafka_streams.integration;


import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;

import com.bakdata.common.kafka.streams.TestRecord;
import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.test_applications.WordCount;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.api.JUnitJupiterSoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public class CleanUpTest {
    private static final int TIMEOUT_SECONDS = 10;
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private EmbeddedKafkaCluster kafkaCluster;
    @RegisterExtension
    JUnitJupiterSoftAssertions softly = new JUnitJupiterSoftAssertions();
    private KafkaStreamsApplication app = null;


    @BeforeEach
    void setup() throws InterruptedException {
        this.kafkaCluster = provisionWith(useDefaults());
        this.kafkaCluster.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        this.app = new WordCount();
        this.app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        final String inputTopicName = "word_input";
        this.app.setInputTopics(List.of(inputTopicName));
        final String outputTopicName = "word_output";
        this.app.setOutputTopic(outputTopicName);
        final String errorTopicName = "word_error";
        this.app.setErrorTopic(errorTopicName);
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setProductive(false);
        this.app.setStreamsConfig(
                Map.of("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"));

        this.kafkaCluster.createTopic(TopicConfig.forTopic(this.app.getOutputTopic()).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic(this.app.getInputTopics().get(0)).useDefaults());
    }

    @AfterEach
    void teardown() throws InterruptedException {
        this.app.close();
        this.app.getStreams().cleanUp();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.kafkaCluster.stop();
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopics().get(0), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        final List<KeyValue<String, Long>> expectedValues =
                List.of(new KeyValue<>("blub", 1L),
                        new KeyValue<>("bla", 1L),
                        new KeyValue<>("blub", 2L)
                );

        this.runAndAssertContent(expectedValues, "WordCount contains all elements after first run");

        this.runCleanUpWithDeletion();

        this.softly.assertThat(this.kafkaCluster.exists(this.app.getOutputTopic()))
                .as("Output topic is deleted")
                .isFalse();

        this.softly.assertThat(this.kafkaCluster.exists(this.app.getErrorTopic()))
                .as("Error topic is deleted")
                .isFalse();
    }

    @Test
    void shouldDeleteState() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopics().get(0), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        final List<KeyValue<String, Long>> expectedValues = List.of(
                new KeyValue<>("blub", 1L),
                new KeyValue<>("bla", 1L),
                new KeyValue<>("blub", 2L)
        );

        this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run");
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.runCleanUp();

        final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                .flatMap(entry -> Stream.of(entry, entry))
                .collect(Collectors.toList());
        this.runAndAssertContent(entriesTwice, "All entries are twice in the input topic after the 2nd run");
    }

    @Test
    void shouldReprocessAlreadySeenRecords() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest =
                SendValuesTransactional.inTransaction(this.app.getInputTopics().get(0),
                        Arrays.asList("a", "b", "c")).useDefaults();
        this.kafkaCluster.send(sendRequest);

        this.runAndAssertSize(3);
        this.runAndAssertSize(3);

        // Wait until all stream application are completely stopped before triggering cleanup
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.runCleanUp();
        this.runAndAssertSize(6);
    }

    @Test
    void shouldDeleteValueSchema() throws InterruptedException, IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();

        this.app.setStreamsConfig(Map.of(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName()
        ));

        final TestRecord testRecord = TestRecord.newBuilder().setContent("val 1").build();

        final SendValuesTransactional<TestRecord> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(testRecord))
                .with("schema.registry.url", this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.app.run();
        this.softly.assertThat(client.getAllSubjects()).hasSize(1);
        this.runCleanUp();
        this.softly.assertThat(client.getAllSubjects()).isEmpty();
    }

    @Test
    void shouldDeleteKeySchema() throws InterruptedException, IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistryMockExtension.getSchemaRegistryClient();

        this.app.setStreamsConfig(Map.of(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName()
        ));

        final TestRecord testRecord = TestRecord.newBuilder().setContent("key 1").build();

        final SendKeyValuesTransactional<TestRecord, String> sendRequest = SendKeyValuesTransactional
                .inTransaction(this.app.getInputTopic(), Collections.singletonList(new KeyValue<>(testRecord, "val")))
                .with("schema.registry.url", this.schemaRegistryMockExtension.getUrl())
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName())
                .build();

        this.kafkaCluster.send(sendRequest);
        this.app.run();
        this.softly.assertThat(client.getAllSubjects()).hasSize(1);
        this.runCleanUp();
        this.softly.assertThat(client.getAllSubjects()).isEmpty();
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

    private void runAndAssertContent(final Iterable<KeyValue<String, Long>> expectedValues, final String description)
            throws InterruptedException {
        this.app.run();
        // Wait until stream application has consumed all data
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.app.close();

        final List<KeyValue<String, Long>> output = this.readOutputTopic(this.app.getOutputTopic());
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void runAndAssertSize(final int expectedMessageCount) throws InterruptedException {
        this.app.run();
        // Wait until stream application has consumed all data
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.app.close();
        final List<KeyValue<String, Long>> records = this.readOutputTopic(this.app.getOutputTopic());
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }


}
