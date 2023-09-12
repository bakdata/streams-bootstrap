/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import static com.bakdata.kafka.util.SchemaClient.createSchemaRegistryClient;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

/**
 * Provide methods for common operations when performing administrative actions on a Kafka cluster
 */
public final class ImprovedAdminClient implements Closeable {

    @Getter
    private final @NonNull Properties properties;
    private final @NonNull AdminClient adminClient;
    private final @NonNull Duration timeout;
    private final SchemaRegistryClient schemaRegistryClient;

    @Builder
    private ImprovedAdminClient(@NonNull final Properties properties,
            final String schemaRegistryUrl, @NonNull final Duration timeout) {
        Preconditions.checkNotNull(properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                "%s must be specified in properties", AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
        this.properties = new Properties(properties);
        this.adminClient = AdminClient.create(properties);
        this.schemaRegistryClient =
                schemaRegistryUrl == null ? null : createSchemaRegistryClient(this.properties, schemaRegistryUrl);
        this.timeout = timeout;
    }

    public Optional<SchemaClient> getSchemaClient() {
        return Optional.ofNullable(this.schemaRegistryClient)
                .map(SchemaClient::new);
    }

    public TopicClient getTopicClient() {
        return new TopicClient(this.adminClient, this.timeout);
    }

    public ConsumerGroupClient getConsumerGroupClient() {
        return new ConsumerGroupClient(this.adminClient, this.timeout);
    }

    public String getBootstrapServers() {
        return this.properties.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    public void close() {
        this.adminClient.close();
    }
}
