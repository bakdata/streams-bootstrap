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

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.test_applications.Mirror;
import com.bakdata.common_kafka_streams.test_applications.WordCount;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
        this.app.setInputTopic(inputTopicName);
        final String outputTopicName = "word_output";
        this.app.setOutputTopic(outputTopicName);
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setProductive(false);
        this.app.setStreamsConfig(
                Map.of("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"));

        this.kafkaCluster.createTopic(TopicConfig.forTopic(this.app.getOutputTopic()).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic(this.app.getInputTopic()).useDefaults());
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
        // create additional topology
        final Mirror mirror = new Mirror();
        mirror.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        final String inputTopicName = "word_output";
        mirror.setInputTopic(inputTopicName);
        final String outputTopicName = "mirror_output";
        mirror.setOutputTopic(outputTopicName);
        mirror.setBrokers(this.kafkaCluster.getBrokerList());
        mirror.setProductive(false);
        mirror.setStreamsConfig(
                Map.of("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        "default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde",
                        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"));

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        final List<KeyValue<String, Long>> expectedValues = List.of(
                new KeyValue<>("blub", 1L),
                new KeyValue<>("bla", 1L),
                new KeyValue<>("blub", 2L)
        );

        this.runAndAssertContent(expectedValues, "First run", this.app.getOutputTopic());
        mirror.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        mirror.close();
        final List<KeyValue<String, Long>> output = this.readOutputTopic(mirror.getOutputTopic());
        this.softly.assertThat(output)
                .as("mirror correct output after first run")
                .containsExactlyInAnyOrderElementsOf(expectedValues);


        this.runCleanUp();
        this.runAndAssertContent(expectedValues, "Second run", this.app.getOutputTopic());

        mirror.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        mirror.close();

        final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                .flatMap(entry -> Stream.of(entry, entry))
                .collect(Collectors.toList());
        final List<KeyValue<String, Long>> output2 = this.readOutputTopic(mirror.getOutputTopic());
        this.softly.assertThat(output2)
                .as("mirror correct output after first run")
                .containsExactlyInAnyOrderElementsOf(entriesTwice);
    }


    @Test
    void shouldDeleteState() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(this.app.getInputTopic(), List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));

        final List<KeyValue<String, Long>> expectedValues = List.of(
                new KeyValue<>("blub", 1L),
                new KeyValue<>("bla", 1L),
                new KeyValue<>("blub", 2L)
        );

        this.runAndAssertContent(expectedValues, "First run", this.app.getOutputTopic());
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.runCleanUp();

        final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                .flatMap(entry -> Stream.of(entry, entry))
                .collect(Collectors.toList());
        this.runAndAssertContent(entriesTwice, "Run after clean up", this.app.getOutputTopic());

    }

    @Test
    void shouldReprocessAlreadySeenRecords() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest =
                SendValuesTransactional.inTransaction(this.app.getInputTopic(),
                        Arrays.asList("a", "b", "c")).useDefaults();
        this.kafkaCluster.send(sendRequest);

        this.runAndAssertSize(3);
        this.runAndAssertSize(3);

        // Wait until all stream application are completely stopped before triggering cleanup
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.runCleanUp();
        this.runAndAssertSize(6);
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

    private void runAndAssertContent(final List<KeyValue<String, Long>> expectedValues, final String description,
            final String outputTopic)
            throws InterruptedException {
        this.app.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.app.close();

        final List<KeyValue<String, Long>> output = this.readOutputTopic(outputTopic);
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
