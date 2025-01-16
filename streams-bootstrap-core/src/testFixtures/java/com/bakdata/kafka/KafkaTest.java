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

package com.bakdata.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.utils.AppInfoParser;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class KafkaTest {
    private final TestTopologyFactory testTopologyFactory = TestTopologyFactory.withSchemaRegistry();
    @Container
    private final KafkaContainer kafkaCluster = newCluster();

    public static KafkaContainer newCluster() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native")
                .withTag(AppInfoParser.getVersion()));
    }

    protected KafkaEndpointConfig createEndpointWithoutSchemaRegistry() {
        return KafkaEndpointConfig.builder()
                .bootstrapServers(this.getBootstrapServers())
                .build();
    }

    protected KafkaEndpointConfig createEndpoint() {
        return KafkaEndpointConfig.builder()
                .bootstrapServers(this.getBootstrapServers())
                .schemaRegistryUrl(this.getSchemaRegistryUrl())
                .build();
    }

    protected String getBootstrapServers() {
        return this.kafkaCluster.getBootstrapServers();
    }

    protected KafkaTestClient newTestClient() {
        return new KafkaTestClient(this.createEndpoint());
    }

    protected String getSchemaRegistryUrl() {
        return this.testTopologyFactory.getSchemaRegistryUrl();
    }

    protected SchemaRegistryClient getSchemaRegistryClient() {
        return this.testTopologyFactory.getSchemaRegistryClient();
    }
}
