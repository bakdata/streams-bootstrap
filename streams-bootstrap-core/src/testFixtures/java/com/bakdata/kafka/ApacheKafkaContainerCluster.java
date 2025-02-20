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

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.kafka.common.Uuid;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

//from https://github.com/testcontainers/testcontainers-java/blob/1404c4429c0cb98fe46534bf33632d25dc5309e4/examples/kafka-cluster/src/test/java/com/example/kafkacluster/ApacheKafkaContainerCluster.java
public class ApacheKafkaContainerCluster implements Startable {

    private final int brokersNum;

    private final Network network;

    @Getter
    private final Collection<KafkaContainer> brokers;

    public ApacheKafkaContainerCluster(final String version, final int brokersNum, final int internalTopicsRf) {
        if (brokersNum < 0) {
            throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
        }
        if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
            throw new IllegalArgumentException(
                    "internalTopicsRf '" + internalTopicsRf + "' must be less than brokersNum and greater than 0"
            );
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        final String controllerQuorumVoters = IntStream.range(0, brokersNum)
                .mapToObj(brokerNum -> String.format("%d@broker-%d:9094", brokerNum, brokerNum))
                .collect(Collectors.joining(","));

        final String clusterId = Uuid.randomUuid().toString();

        final DockerImageName dockerImageName = DockerImageName.parse("apache/kafka").withTag(version);
        this.brokers = IntStream.range(0, brokersNum)
                .mapToObj(brokerNum -> new KafkaContainer(dockerImageName)
                        .withNetwork(this.network)
                        .withNetworkAliases("broker-" + brokerNum)
                        .withEnv("CLUSTER_ID", clusterId)
                        .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                        .withEnv("KAFKA_NODE_ID", brokerNum + "")
                        .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters)
                        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                        .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + "")
                        .withEnv("KAFKA_LISTENERS",
                                "PLAINTEXT://:9092,BROKER://:9093,CONTROLLER://:9094") //TODO remove with 3.9.1 https://issues.apache.org/jira/browse/KAFKA-18281
                        .withStartupTimeout(Duration.ofMinutes(1)))
                .collect(Collectors.toList());
    }

    private static int getNumberOfReadyBrokers(final ContainerState container)
            throws IOException, InterruptedException {
        final Container.ExecResult result = container
                .execInContainer(
                        "sh",
                        "-c",
                        "/opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9093 --describe | "
                        + "grep -o '\"broker\"' | "
                        + "wc -l"
                );
        final String brokers = result.getStdout().replace("\n", "");

        return Integer.parseInt(brokers);
    }

    public String getBootstrapServers() {
        return this.brokers.stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
    }

    @Override
    public void start() {
        // Needs to start all the brokers at once
        this.brokers.parallelStream().forEach(GenericContainer::start);

        await()
                .atMost(Duration.ofSeconds(120))
                .until(() -> {
                    final KafkaContainer container = this.brokers.stream()
                            .findFirst()
                            .orElseThrow();
                    return getNumberOfReadyBrokers(container);
                }, readyBrokers -> readyBrokers == this.brokersNum);
    }

    @Override
    public void stop() {
        this.brokers.stream().parallel().forEach(GenericContainer::stop);

        this.network.close();
    }
}
