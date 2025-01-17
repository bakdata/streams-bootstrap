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

package com.bakdata.kafka.util;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

class TopicClientTest extends KafkaTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10L);

    @Test
    void shouldNotFindTopic() {
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("does_not_exist")).isFalse();
        }
    }

    @Test
    void shouldFindTopic() {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("exists", KafkaTestClient.defaultTopicSettings().build());
            assertThat(client.exists("exists")).isTrue();
        }
    }

    @Test
    void shouldListTopics() {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("foo", KafkaTestClient.defaultTopicSettings().build());
            client.createTopic("bar", KafkaTestClient.defaultTopicSettings().build());
            assertThat(client.listTopics())
                            .hasSize(2)
                    .containsExactlyInAnyOrder("foo", "bar");
        }
    }

    @Test
    void shouldDeleteTopic() {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("foo", KafkaTestClient.defaultTopicSettings().build());
            assertThat(client.exists("foo")).isTrue();
            client.deleteTopic("foo");
            assertThat(client.listTopics())
                    .isEmpty();
        }
    }

    @Test
    void shouldCreateTopic() {
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("topic")).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
//                    .replicationFactor((short) 2) // FIXME setup testcontainers with multiple brokers
                    .replicationFactor((short) 1)
                    .build();
            client.createTopic("topic", settings, emptyMap());
            assertThat(client.exists("foo")).isTrue();
            assertThat(client.describe("topic"))
                    .satisfies(info -> {
                        assertThat(info.getReplicationFactor()).isEqualTo((short) 1);
                        assertThat(info.getPartitions()).isEqualTo(5);
                    });
        }
    }

    private TopicClient createClient() {
        final String brokerList = this.getBootstrapServers();
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return TopicClient.create(config, CLIENT_TIMEOUT);
    }

}
