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

import static com.bakdata.kafka.KafkaTestClient.defaultTopicSettings;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.admin.TopicsClient.TopicClient;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

class TopicsClientTest extends KafkaTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10L);

    @Test
    void shouldNotFindTopic() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            assertThat(client.topic("does_not_exist").exists()).isFalse();
        }
    }

    @Test
    void shouldNotDescribeTopic() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            assertThat(client.topic("does_not_exist").describe()).isNotPresent();
        }
    }

    @Test
    void shouldNotGetTopicConfigs() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            assertThat(client.topic("does_not_exist").config().describe()).isEmpty();
        }
    }

    @Test
    void shouldFindTopic() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient exists = client.topic("exists");
            exists.create(defaultTopicSettings().build());
            assertThat(client.topic("exists").exists()).isTrue();
        }
    }

    @Test
    void shouldListTopics() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            client.topic("foo").create(defaultTopicSettings().build());
            client.topic("bar").create(defaultTopicSettings().build());
            assertThat(client.list())
                    .hasSize(2)
                    .containsExactlyInAnyOrder("foo", "bar");
        }
    }

    @Test
    void shouldDeleteTopic() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient foo = client.topic("foo");
            foo.create(defaultTopicSettings().build());
            assertThat(foo.exists()).isTrue();
            foo.delete();
            assertThat(client.list())
                    .isEmpty();
        }
    }

    @Test
    void shouldCreateTopic() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient topic = client.topic("topic");
            assertThat(topic.exists()).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
                    .replicationFactor((short) 1)
                    .build();
            topic.create(settings, emptyMap());
            assertThat(topic.exists()).isTrue();
            assertThat(topic.getSettings())
                    .hasValueSatisfying(info -> {
                        assertThat(info.getReplicationFactor()).isEqualTo((short) 1);
                        assertThat(info.getPartitions()).isEqualTo(5);
                    });
        }
    }

    @Test
    void shouldCreateTopicIfNotExists() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient topic = client.topic("topic");
            assertThat(topic.exists()).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
                    .replicationFactor((short) 1)
                    .build();
            topic.createIfNotExists(settings);
            assertThat(topic.exists()).isTrue();
            assertThat(topic.getSettings())
                    .hasValueSatisfying(info -> {
                        assertThat(info.getReplicationFactor()).isEqualTo((short) 1);
                        assertThat(info.getPartitions()).isEqualTo(5);
                    });
        }
    }

    @Test
    void shouldNotCreateTopicIfExists() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final TopicClient topic = client.topic("topic");
            assertThat(topic.exists()).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
                    .replicationFactor((short) 1)
                    .build();
            topic.create(settings);
            final TopicSettings newSettings = TopicSettings.builder()
                    .partitions(4)
                    .replicationFactor((short) 2)
                    .build();
            topic.createIfNotExists(newSettings);
            assertThat(topic.exists()).isTrue();
            assertThat(topic.getSettings())
                    .hasValueSatisfying(info -> {
                        assertThat(info.getReplicationFactor()).isEqualTo((short) 1);
                        assertThat(info.getPartitions()).isEqualTo(5);
                    });
        }
    }

    @Test
    void shouldGetTopicConfig() {
        try (final AdminClientX admin = this.createAdminClient()) {
            final TopicsClient client = admin.topics();
            final Map<String, String> config = Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG,
                    TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE
            );
            final TopicClient foo = client.topic("foo");
            foo.create(defaultTopicSettings().build(), config);
            assertThat(foo.config().describe())
                    .containsEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                            TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE);
        }
    }

    private AdminClientX createAdminClient() {
        final String brokerList = this.getBootstrapServers();
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return AdminClientX.create(config, CLIENT_TIMEOUT);
    }

}
