/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import static com.bakdata.kafka.util.SchemaTopicClient.createSchemaRegistryClient;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/**
 * Provide methods for common operations when performing administrative actions on a Kafka cluster
 */
@Builder(access = AccessLevel.PRIVATE)
public final class ImprovedAdminClient implements Closeable {

    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(10L);
    @Getter
    private final @NonNull AdminClient adminClient;
    private final SchemaRegistryClient schemaRegistryClient;
    private final @NonNull Duration timeout;

    public static ImprovedAdminClient create(@NonNull final Map<String, Object> properties) {
        return create(properties, ADMIN_TIMEOUT);
    }

    public static ImprovedAdminClient create(@NonNull final Map<String, Object> properties,
            @NonNull final Duration timeout) {
        Preconditions.checkNotNull(properties.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                "%s must be specified in properties", AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        final AdminClient adminClient = AdminClient.create(properties);
        final String schemaRegistryUrl =
                (String) properties.get(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryUrl == null ? null : createSchemaRegistryClient(properties, schemaRegistryUrl);
        return builder()
                .adminClient(adminClient)
                .schemaRegistryClient(schemaRegistryClient)
                .timeout(timeout)
                .build();
    }

    public Optional<SchemaRegistryClient> getSchemaRegistryClient() {
        return Optional.ofNullable(this.schemaRegistryClient);
    }

    public SchemaTopicClient getSchemaTopicClient() {
        return new SchemaTopicClient(this.getTopicClient(), this.schemaRegistryClient);
    }

    public TopicClient getTopicClient() {
        return new TopicClient(this.adminClient, this.timeout);
    }

    public ConsumerGroupClient getConsumerGroupClient() {
        return new ConsumerGroupClient(this.adminClient, this.timeout);
    }

    @Override
    public void close() {
        this.adminClient.close();
    }
}
