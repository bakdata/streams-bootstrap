/*
 * MIT LICENCE
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
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.TopicCleaner;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class DeleteTopicTest {
    private static final int TIMEOUT_SECONDS = 10;
    private static final String TOPIC = "topic";
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private EmbeddedKafkaCluster kafkaCluster = null;
    private TopicCleaner topicCleaner = null;

    @BeforeEach
    void setup() {
        this.kafkaCluster = provisionWith(useDefaults());
        this.kafkaCluster.start();

        final Properties kafkaProperties = new Properties();
        kafkaProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaCluster.getBrokerList());

        this.topicCleaner = TopicCleaner.create(kafkaProperties, this.schemaRegistryMockExtension.getUrl());
    }

    @AfterEach
    void teardown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        this.kafkaCluster.createTopic(TopicConfig.forTopic(TOPIC).useDefaults());
        assertThat(this.kafkaCluster.exists(TOPIC))
                .as("Topic is created")
                .isTrue();

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction(TOPIC, List.of("blub", "bla", "blub"))
                .useDefaults();
        this.kafkaCluster.send(sendRequest);
        this.topicCleaner.deleteTopic(TOPIC);

        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(this.kafkaCluster.exists(TOPIC))
                .as("Topic is deleted")
                .isFalse();
    }

}
