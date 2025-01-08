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

package com.bakdata.kafka.integration;

import static java.util.Collections.emptyMap;

import com.bakdata.kafka.KafkaContainerHelper;
import com.bakdata.kafka.KafkaEndpointConfig;
import com.bakdata.kafka.TestUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import java.util.List;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
abstract class KafkaTest {
    static final String SCHEMA_REGISTRY_URL = "mock://";
    @Container
    private final KafkaContainer kafkaCluster = TestUtil.newKafkaCluster();

    static SchemaRegistryClient getSchemaRegistryClient() {
        return SchemaRegistryClientFactory.newClient(List.of(SCHEMA_REGISTRY_URL), 0, null, emptyMap(), null);
    }

    KafkaEndpointConfig createEndpointWithoutSchemaRegistry() {
        return KafkaEndpointConfig.builder()
                .bootstrapServers(this.kafkaCluster.getBootstrapServers())
                .build();
    }

    KafkaEndpointConfig createEndpoint() {
        return KafkaEndpointConfig.builder()
                .bootstrapServers(this.kafkaCluster.getBootstrapServers())
                .schemaRegistryUrl(SCHEMA_REGISTRY_URL)
                .build();
    }

    KafkaContainerHelper newContainerHelper() {
        return new KafkaContainerHelper(this.kafkaCluster);
    }
}
