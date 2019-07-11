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

import com.bakdata.common_kafka_streams.test_applications.Mirror;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import java.util.Arrays;
import java.util.List;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.assertj.core.api.JUnitJupiterSoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class ReprocessingTest {
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private final EmbeddedKafkaCluster kafkaCluster = provisionWith(useDefaults());
    @RegisterExtension
    JUnitJupiterSoftAssertions softly = new JUnitJupiterSoftAssertions();
    private Mirror mirror;


    @BeforeEach
    void setup() {
        this.kafkaCluster.start();

        this.mirror = new Mirror();
        this.mirror.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        final String inputTopicName = "input";
        this.mirror.setInputTopic(inputTopicName);
        final String outputTopicName = "output";
        this.mirror.setOutputTopic(outputTopicName);
        this.mirror.setBrokers(this.kafkaCluster.getBrokerList());
        this.mirror.setProductive(false);

        this.kafkaCluster
                .createTopic(
                        TopicConfig.forTopic(this.mirror.getOutputTopic()).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic(this.mirror.getInputTopic()).useDefaults());
    }

    @AfterEach
    void teardown() {
        this.mirror.close();
        this.kafkaCluster.stop();
    }

    @Test
    void shouldReprocessAlreadySeenRecords() throws InterruptedException {
        final SendValuesTransactional<String> sendRequest =
                SendValuesTransactional.inTransaction(this.mirror.getInputTopic(),
                        Arrays.asList("a", "b", "c")).useDefaults();
        this.kafkaCluster.send(sendRequest);

        this.runAndAssert(3);

        this.runAndAssert(3);

        Thread.sleep(10000);
        this.mirror.setForceReprocessing(true);
        this.runAndAssert(6);
    }

    private List<KeyValue<String, String>> readFromTopic(String topic) throws InterruptedException {
        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from(topic).useDefaults();
        return this.kafkaCluster.read(readRequest);
    }

    private void runAndAssert(final int expectedMessageCount) throws InterruptedException {
        this.mirror.run();
        Thread.sleep(5000);
        this.mirror.close();
        final List<KeyValue<String, String>> records = this.readFromTopic(this.mirror.getOutputTopic());
        this.softly.assertThat(records).hasSize(expectedMessageCount);

    }

}
