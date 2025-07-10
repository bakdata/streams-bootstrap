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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.admin.ConsumerGroupClient.ForGroup;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.junit.jupiter.api.Test;

class ConsumerGroupClientTest extends KafkaTest {

    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(10L);

    @Test
    void shouldNotFindGroup() {
        try (final ConsumerGroupClient client = this.createClient()) {
            assertThat(client.forGroup("does_not_exist").exists()).isFalse();
        }
    }

    @Test
    void shouldNotDescribeGroup() {
        try (final ConsumerGroupClient client = this.createClient()) {
            assertThat(client.forGroup("does_not_exist").describe()).isNotPresent();
        }
    }

    @Test
    void shouldNotListOffsets() {
        try (final ConsumerGroupClient client = this.createClient()) {
            assertThat(client.forGroup("does_not_exist").listOffsets()).isEmpty();
        }
    }

    @Test
    void shouldAddGroupConfigs() {
        try (final ConsumerGroupClient client = this.createClient()) {
            final ForGroup group = client.forGroup("group");
            group.addConfig(
                    new ConfigEntry("consumer.session.timeout.ms", Long.toString(Duration.ofSeconds(10L).toMillis())));
            assertThat(group.getConfig())
                    .containsEntry("consumer.session.timeout.ms", Long.toString(Duration.ofSeconds(10L).toMillis()));
        }
    }

    private ConsumerGroupClient createClient() {
        final String brokerList = this.getBootstrapServers();
        final Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return ConsumerGroupClient.create(config, CLIENT_TIMEOUT);
    }

}
