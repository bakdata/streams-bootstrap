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

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/**
 * This class offers helpers to interact with Kafka consumer groups.
 */
@RequiredArgsConstructor
@Slf4j
public final class ConsumerGroupClient implements AutoCloseable {

    private final @NonNull Admin adminClient;
    private final @NonNull Duration timeout;

    /**
     * Creates a new {@code ConsumerGroupClient} using the specified configuration.
     *
     * @param configs properties passed to {@link AdminClient#create(Map)}
     * @param timeout timeout for waiting for Kafka admin calls
     * @return {@code ConsumerGroupClient}
     */
    public static ConsumerGroupClient create(final Map<String, Object> configs, final Duration timeout) {
        return new ConsumerGroupClient(AdminClient.create(configs), timeout);
    }

    private static KafkaAdminException failedToDeleteGroup(final String groupName, final Throwable ex) {
        return new KafkaAdminException("Failed to delete consumer group " + groupName, ex);
    }

    /**
     * Delete a consumer group.
     *
     * @param groupName the consumer group name
     */
    public void deleteConsumerGroup(final String groupName) {
        log.info("Deleting consumer group '{}'", groupName);
        try {
            this.adminClient.deleteConsumerGroups(List.of(groupName))
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
            log.info("Deleted consumer group '{}'", groupName);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw failedToDeleteGroup(groupName, ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw failedToDeleteGroup(groupName, ex);
        } catch (final TimeoutException ex) {
            throw failedToDeleteGroup(groupName, ex);
        }
    }

    @Override
    public void close() {
        this.adminClient.close();
    }

    /**
     * Checks whether a Kafka consumer group exists.
     *
     * @param groupName the consumer group name
     * @return whether a Kafka consumer group with the specified name exists or not
     */
    public boolean exists(final String groupName) {
        final Collection<ConsumerGroupListing> consumerGroups = this.listGroups();
        return consumerGroups.stream()
                .anyMatch(c -> c.groupId().equals(groupName));
    }

    /**
     * List consumer groups.
     *
     * @return consumer groups
     */
    public Collection<ConsumerGroupListing> listGroups() {
        try {
            return this.adminClient
                    .listConsumerGroups()
                    .all()
                    .get(this.timeout.toSeconds(), TimeUnit.SECONDS);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("Failed to list consumer groups", ex);
        } catch (final ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw new KafkaAdminException("Failed to list consumer groups", ex);
        } catch (final TimeoutException ex) {
            throw new KafkaAdminException("Failed to list consumer groups", ex);
        }
    }

    /**
     * Delete a consumer group only if it exists.
     *
     * @param groupName the consumer group name
     */
    public void deleteGroupIfExists(final String groupName) {
        if (this.exists(groupName)) {
            try {
                this.deleteConsumerGroup(groupName);
            } catch (final GroupIdNotFoundException e) {
                // do nothing
            }
        }
    }
}
