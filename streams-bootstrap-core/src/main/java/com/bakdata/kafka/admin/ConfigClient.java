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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

@RequiredArgsConstructor
class ConfigClient {
    private final @NonNull Admin adminClient;
    private final @NonNull Duration timeout;

    ForResource forResource(final ConfigResource resource) {
        return new ForResource(resource);
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    class ForResource {
        private final @NonNull ConfigResource resource;

        Map<String, String> getConfigs() {
            try {
                final Map<ConfigResource, KafkaFuture<Config>> configMap =
                        ConfigClient.this.adminClient.describeConfigs(List.of(this.resource)).values();
                final Config config =
                        configMap.get(this.resource).get(ConfigClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
                return config.entries().stream()
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return emptyMap();
                }
                if (e.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToRetrieveConfig(e);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw this.failedToRetrieveConfig(e);
            } catch (final TimeoutException e) {
                throw this.failedToRetrieveConfig(e);
            }
        }

        void addConfig(final ConfigEntry configEntry) {
            final AlterConfigOp alterConfig = new AlterConfigOp(configEntry, OpType.SET);
            final Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(this.resource, List.of(alterConfig));
            try {
                ConfigClient.this.adminClient.incrementalAlterConfigs(configs).all()
                        .get(ConfigClient.this.timeout.toSeconds(), TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw this.failedToAddConfigs(e);
            } catch (final ExecutionException e) {
                if (e.getCause() instanceof final RuntimeException cause) {
                    throw cause;
                }
                throw this.failedToAddConfigs(e);
            } catch (final TimeoutException e) {
                throw this.failedToAddConfigs(e);
            }
        }

        private KafkaAdminException failedToRetrieveConfig(final Throwable e) {
            return new KafkaAdminException(
                    "Failed to retrieve config of " + this.resource.type().name().toLowerCase() + " "
                    + this.resource.name(), e);
        }

        private KafkaAdminException failedToAddConfigs(final Throwable e) {
            return new KafkaAdminException(
                    "Failed to add config to " + this.resource.type().name().toLowerCase() + " " + this.resource.name(),
                    e);
        }
    }
}
