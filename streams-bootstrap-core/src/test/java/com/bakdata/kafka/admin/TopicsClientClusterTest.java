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

package com.bakdata.kafka.admin;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.bakdata.kafka.ApacheKafkaContainerCluster;
import com.bakdata.kafka.admin.TopicsClient.TopicClient;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.utils.AppInfoParser;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class TopicsClientClusterTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10L);
    @Container
    private final ApacheKafkaContainerCluster kafkaCluster =
            new ApacheKafkaContainerCluster(AppInfoParser.getVersion(), 3, 2);

    @Test
    void shouldCreateTopicWithReplication() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient topic = client.topic("topic");
            assertThat(topic.exists()).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
                    .replicationFactor((short) 2)
                    .build();
            topic.create(settings, emptyMap());
            // topic needs to be propagated to all KRaft controllers
            // topic creation only verifies existence on one controller
            await()
                    .pollDelay(Duration.ofSeconds(1L))
                    .atMost(Duration.ofSeconds(20L))
                    .untilAsserted(() -> {
                        assertThat(topic.exists()).isTrue();
                        assertThat(topic.getSettings())
                                .hasValueSatisfying(info -> {
                                    assertThat(info.getReplicationFactor()).isEqualTo((short) 2);
                                    assertThat(info.getPartitions()).isEqualTo(5);
                                });
                    });
        }
    }

    private AdminClientX createAdminClient() {
        final String brokerList = this.kafkaCluster.getBootstrapServers();
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return AdminClientX.create(config, CLIENT_TIMEOUT);
    }

}
