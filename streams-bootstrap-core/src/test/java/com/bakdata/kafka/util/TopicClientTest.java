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

import static com.bakdata.kafka.KafkaTestClient.defaultTopicSettings;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
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
    void shouldFindTopic() throws InterruptedException {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("exists", defaultTopicSettings().build());
        }
        Thread.sleep(CLIENT_TIMEOUT.toMillis());
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("exists")).isTrue();
        }
    }

    @Test
    void shouldListTopics() throws InterruptedException {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("foo", defaultTopicSettings().build());
            client.createTopic("bar", defaultTopicSettings().build());
        }
        Thread.sleep(CLIENT_TIMEOUT.toMillis());
        try (final TopicClient client = this.createClient()) {
            assertThat(client.listTopics())
                    .hasSize(2)
                    .containsExactlyInAnyOrder("foo", "bar");
        }
    }

    @Test
    void shouldDeleteTopic() throws InterruptedException {
        try (final TopicClient client = this.createClient()) {
            client.createTopic("foo", defaultTopicSettings().build());
        }
        Thread.sleep(CLIENT_TIMEOUT.toMillis());
        try (final TopicClient client = this.createClient()) {
            assertThat(client.listTopics())
                    .hasSize(1)
                    .containsExactlyInAnyOrder("foo");
            client.deleteTopic("foo");
            assertThat(client.listTopics())
                    .isEmpty();
        }
    }

    @Test
    void shouldCreateTopic() throws InterruptedException {
        try (final TopicClient client = this.createClient()) {
            assertThat(client.exists("topic")).isFalse();
            final TopicSettings settings = TopicSettings.builder()
                    .partitions(5)
//                    .replicationFactor((short) 2) // FIXME setup testcontainers with multiple brokers
                    .replicationFactor((short) 1)
                    .build();
            client.createTopic("topic", settings, emptyMap());
            Thread.sleep(CLIENT_TIMEOUT.toMillis());
            assertThat(client.exists("topic")).isTrue();
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
