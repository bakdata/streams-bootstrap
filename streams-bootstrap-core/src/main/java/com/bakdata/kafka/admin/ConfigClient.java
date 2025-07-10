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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

@RequiredArgsConstructor
class ConfigClient {
    private final @NonNull Admin adminClient;
    private final @NonNull Duration timeout;

    private static KafkaAdminException failedToRetrieveConfig(final String entityName, final Type type,
            final Throwable e) {
        return new KafkaAdminException("Failed to retrieve config of " + type.name().toLowerCase() + " " + entityName,
                e);
    }

    private static KafkaAdminException failedToAddConfigs(final String entityName, final Type type, final Throwable e) {
        return new KafkaAdminException("Failed to add config to " + type.name().toLowerCase() + " " + entityName, e);
    }

    Map<String, String> getConfigs(final Type type, final String entityName) {
        try {
            final ConfigResource configResource = new ConfigResource(type, entityName);
            final Map<ConfigResource, KafkaFuture<Config>> configMap =
                    this.adminClient.describeConfigs(List.of(configResource)).values();
            final Config config = configMap.get(configResource).get(this.timeout.toSeconds(), TimeUnit.SECONDS);
            return config.entries().stream()
                    .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return emptyMap();
            }
            if (e.getCause() instanceof final RuntimeException cause) {
                throw cause;
            }
            throw failedToRetrieveConfig(entityName, type, e);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failedToRetrieveConfig(entityName, type, e);
        } catch (final TimeoutException e) {
            throw failedToRetrieveConfig(entityName, type, e);
        }
    }

    void addConfig(final Type type, final String entityName, final ConfigEntry configEntry) {
        final ConfigResource configResource = new ConfigResource(type, entityName);
        final AlterConfigOp alterConfig = new AlterConfigOp(configEntry, OpType.SET);
        final Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(configResource, List.of(alterConfig));
        try {
            this.adminClient.incrementalAlterConfigs(configs).all().get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw failedToAddConfigs(entityName, type, e);
        } catch (final ExecutionException e) {
            if (e.getCause() instanceof final RuntimeException cause) {
                throw cause;
            }
            throw failedToAddConfigs(entityName, type, e);
        } catch (final TimeoutException e) {
            throw failedToAddConfigs(entityName, type, e);
        }
    }
}
