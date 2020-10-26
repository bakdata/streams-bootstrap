/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyMap;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaConfig;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopicClientTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10L);
    private final EmbeddedKafkaCluster kafkaCluster = createKafkaCluster();

    private static EmbeddedKafkaCluster createKafkaCluster() {
        final EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.create()
                .withNumberOfBrokers(2)
                .build();
        final EmbeddedKafkaClusterConfig clusterConfig = EmbeddedKafkaClusterConfig.create()
                .provisionWith(kafkaConfig)
                .build();
        return provisionWith(clusterConfig);
    }

    @BeforeEach
    void setup() throws InterruptedException {
        this.kafkaCluster.start();
        Thread.sleep(Duration.ofSeconds(20).toMillis());
    }

    @AfterEach
    void teardown() {
        this.kafkaCluster.stop();
    }

    @Test
    void shouldNotFindTopic() {
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("does_not_exist")).isFalse();
        }
    }

    @Test
    void shouldFindTopic() {
        this.kafkaCluster.createTopic(TopicConfig.forTopic("exists").useDefaults());
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("exists")).isTrue();
        }
    }

    @Test
    void shouldListTopics() {
        this.kafkaCluster.createTopic(TopicConfig.forTopic("foo").useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic("bar").useDefaults());
        try (final TopicClient client = this.createClient()) {
            assertThat(client.listTopics())
                    .hasSize(2)
                    .containsExactlyInAnyOrder("foo", "bar");
        }
    }

    @Test
    void shouldCreateTopic() {
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("topic")).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
                    .replicationFactor((short) 2)
                    .build();
            client.createTopic("topic", settings, emptyMap());
            assertThat(client.exists("topic")).isTrue();
            assertThat(client.describe("topic"))
                    .satisfies(info -> {
                        assertThat(info.getReplicationFactor()).isEqualTo((short) 2);
                        assertThat(info.getPartitions()).isEqualTo(5);
                    });
        }
    }

    private TopicClient createClient() {
        final String brokerList = this.kafkaCluster.getBrokerList();
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return TopicClient.create(config, CLIENT_TIMEOUT);
    }

}
