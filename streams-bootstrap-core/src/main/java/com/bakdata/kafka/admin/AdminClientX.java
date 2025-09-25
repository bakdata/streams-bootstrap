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

import java.time.Duration;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/**
 * Provide methods for common operations when performing administrative actions on a Kafka cluster
 */
@Builder(access = AccessLevel.PRIVATE)
public final class AdminClientX implements AutoCloseable {

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10L);
    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    /**
     * Create a new admin client with default timeout
     * @param properties Kafka configuration
     * @return admin client
     */
    public static AdminClientX create(@NonNull final Map<String, Object> properties) {
        return create(properties, ADMIN_TIMEOUT);
    }

    /**
     * Create a new admin client
     * @param properties Kafka configuration
     * @param timeout timeout when performing admin operations
     * @return admin client
     */
    public static AdminClientX create(@NonNull final Map<String, Object> properties,
            @NonNull final Duration timeout) {
        if (!properties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(
                    String.format("%s must be specified in properties", AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        }
        final Admin adminClient = AdminClient.create(properties);
        return builder()
                .adminClient(adminClient)
                .timeout(new Timeout(timeout))
                .build();
    }

    /**
     * Get the underlying {@link Admin} client.
     *
     * @return admin client
     */
    public Admin admin() {
        return new PooledAdmin(this.adminClient);
    }

    /**
     * Create a {@link TopicsClient} to perform topic-related administrative actions.
     * @return topic client
     */
    public TopicsClient topics() {
        return new TopicsClient(this.adminClient, this.timeout);
    }

    /**
     * Create a {@link ConsumerGroupsClient} to perform consumer group-related administrative actions
     * @return consumer group client
     */
    public ConsumerGroupsClient consumerGroups() {
        return new ConsumerGroupsClient(this.adminClient, this.timeout);
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    @RequiredArgsConstructor
    private static class PooledAdmin implements Admin {
        @Delegate(excludes = AutoCloseable.class)
        private final @NonNull Admin admin;

        @Override
        public void close(final Duration timeout) {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
