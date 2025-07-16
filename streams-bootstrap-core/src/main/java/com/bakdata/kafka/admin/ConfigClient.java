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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * This class offers helpers to interact with Kafka resource configs.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class ConfigClient implements AutoCloseable {
    private final @NonNull Admin adminClient;
    private final @NonNull Timeout timeout;

    /**
     * Create a new config client
     *
     * @param adminClient admin client
     * @param timeout timeout when performing admin operations
     */
    public ConfigClient(final Admin adminClient, final Duration timeout) {
        this(adminClient, new Timeout(timeout));
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    /**
     * Create a client for a specific resource.
     *
     * @param resource resource
     * @return a config client for the specified resource
     */
    public ForResource forResource(final ConfigResource resource) {
        return new ForResource(resource);
    }

    /**
     * A client for a specific Kafka resource.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class ForResource {
        private final @NonNull ConfigResource resource;

        /**
         * Describes the current configuration of a Kafka resource.
         *
         * @return config of resource
         */
        public Map<String, String> describe() {
            try {
                final DescribeConfigsResult result =
                        ConfigClient.this.adminClient.describeConfigs(List.of(this.resource));
                final Map<ConfigResource, KafkaFuture<Config>> configMap = result.values();
                final KafkaFuture<Config> future = configMap.get(this.resource);
                final Config config = ConfigClient.this.timeout.get(future, this::failedToGet);
                return config.entries().stream()
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
            } catch (final UnknownTopicOrPartitionException e) {
                // topic does not exist
                return emptyMap();
            }
        }

        /**
         * Add a config for a Kafka resource.
         *
         * @param configEntry the configuration entry to add
         */
        public void add(final ConfigEntry configEntry) {
            final AlterConfigOp alterConfig = new AlterConfigOp(configEntry, OpType.SET);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(this.resource, List.of(alterConfig));
            final AlterConfigsResult result = ConfigClient.this.adminClient.incrementalAlterConfigs(configs);
            ConfigClient.this.timeout.get(result.all(), this::failedToAdd);
        }

        private KafkaAdminException failedToGet(final Throwable e) {
            return new KafkaAdminException("Failed to describe config of " + this.getName(), e);
        }

        private KafkaAdminException failedToAdd(final Throwable e) {
            return new KafkaAdminException("Failed to add config to " + this.getName(), e);
        }

        private String getName() {
            return this.resource.type().name().toLowerCase(Locale.getDefault()) + " " + this.resource.name();
        }
    }
}
