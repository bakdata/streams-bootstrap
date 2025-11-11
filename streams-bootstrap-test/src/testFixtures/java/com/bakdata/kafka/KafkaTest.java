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

import com.bakdata.kafka.consumer.ExecutableConsumerApp;
import com.bakdata.kafka.consumerproducer.ExecutableConsumerProducerApp;
import com.bakdata.kafka.streams.ExecutableStreamsApp;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.kafka.common.utils.AppInfoParser;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class KafkaTest {
    public static final Duration POLL_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration SESSION_TIMEOUT = Duration.ofSeconds(10L);
    @Getter(AccessLevel.PROTECTED)
    private final TestSchemaRegistry schemaRegistry = new TestSchemaRegistry();
    @Container
    private final KafkaContainer kafkaCluster = newCluster();

    public static KafkaContainer newCluster() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka") //FIXME native image is flaky
                .withTag(AppInfoParser.getVersion()));
    }

    public static void awaitProcessing(final ExecutableStreamsApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        awaitProcessing(verifier);
    }

    protected static void awaitProcessing(final ConsumerGroupVerifier verifier) {
        awaitActive(verifier);
        await()
                .alias("Consumer group has finished processing")
                .until(verifier::hasFinishedProcessing);
    }

    public static void awaitActive(final ExecutableStreamsApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        awaitActive(verifier);
    }

    protected static void awaitActive(final ConsumerGroupVerifier verifier) {
        await()
                .alias("Consumer group is active")
                .until(verifier::isActive);
    }

    protected static void awaitClosed(final ExecutableStreamsApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        awaitClosed(verifier);
    }

    protected static void awaitClosed(final ConsumerGroupVerifier verifier) {
        await()
                .alias("Consumer group is closed")
                .until(verifier::isClosed);
    }

    public static void awaitProcessing(final ExecutableConsumerApp<?> app) {
        awaitActive(app);
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group has finished processing")
                .until(verifier::hasFinishedProcessing);
    }

    protected static void awaitActive(final ExecutableConsumerApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group is active")
                .until(verifier::isActive);
    }

    protected static void awaitClosed(final ExecutableConsumerApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group is closed")
                .until(verifier::isClosed);
    }

    public static void awaitProcessing(final ExecutableConsumerProducerApp<?> app) {
        awaitActive(app);
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group has finished processing")
                .until(verifier::hasFinishedProcessing);
    }

    protected static void awaitActive(final ExecutableConsumerProducerApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group is active")
                .until(verifier::isActive);
    }

    protected static void awaitClosed(final ExecutableConsumerProducerApp<?> app) {
        final ConsumerGroupVerifier verifier = ConsumerGroupVerifier.verify(app);
        await()
                .alias("Consumer group is closed")
                .until(verifier::isClosed);
    }

    private static ConditionFactory await() {
        return Awaitility.await()
                .pollInterval(Duration.ofSeconds(2L))
                .atMost(Duration.ofSeconds(20L));
    }

    protected RuntimeConfiguration createConfig() {
        return RuntimeConfiguration.create(this.getBootstrapServers());
    }

    protected RuntimeConfiguration createConfigWithSchemaRegistry() {
        return this.createConfig()
                .withSchemaRegistryUrl(this.getSchemaRegistryUrl());
    }

    protected String getBootstrapServers() {
        return this.kafkaCluster.getBootstrapServers();
    }

    protected KafkaTestClient newTestClient() {
        return new KafkaTestClient(this.createConfigWithSchemaRegistry());
    }

    protected String getSchemaRegistryUrl() {
        return this.schemaRegistry.getSchemaRegistryUrl();
    }

    protected SchemaRegistryClient getSchemaRegistryClient() {
        return this.schemaRegistry.getSchemaRegistryClient();
    }

}
