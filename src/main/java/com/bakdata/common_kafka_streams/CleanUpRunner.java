package com.bakdata.common_kafka_streams;

import static com.bakdata.common_kafka_streams.KafkaStreamsApplication.RESET_SLEEP_MS;

import com.bakdata.common_kafka_streams.util.TopologyInformation;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import kafka.tools.StreamsResetter;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;


@Slf4j
public class CleanUpRunner {
    private final String appId;
    private final Properties kafkaProperties;
    private final SchemaRegistryClient client;
    private final List<String> inputTopics;
    private final String brokers;
    private final KafkaStreams streams;
    private final TopologyInformation topologyInformation;

    @Builder
    public CleanUpRunner(final @NonNull Topology topology, final @NonNull String appId,
            final @NonNull Properties kafkaProperties, final @NonNull String schemaRegistryUrl,
            final @NonNull List<String> inputTopics, final @NonNull String brokers,
            final @NonNull KafkaStreams streams) {
        this.appId = appId;
        this.kafkaProperties = kafkaProperties;
        this.client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        this.inputTopics = inputTopics;
        this.brokers = brokers;
        this.streams = streams;
        this.topologyInformation = new TopologyInformation(topology, appId);
    }

    public void run(final boolean deleteOutputTopic) {
        runResetter(String.join(",", this.inputTopics), this.brokers, this.appId);
        if (deleteOutputTopic) {
            this.deleteTopics();
        }
        this.streams.cleanUp();
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    protected static void runResetter(final String inputTopics, final String brokers, final String appId) {
        final String[] args = {
                "--application-id", appId,
                "--bootstrap-servers", brokers,
                "--input-topics", inputTopics
        };
        final StreamsResetter resetter = new StreamsResetter();
        resetter.run(args);
    }

    protected void deleteTopics() {
        // the StreamsResetter is responsible for deleting internal topics
        this.topologyInformation.getInternalTopics().forEach(this::resetSchemaRegistry);
        final List<String> externalTopics = this.topologyInformation.getExternalSinkTopics();
        externalTopics.forEach(this::deleteTopic);
        externalTopics.forEach(this::resetSchemaRegistry);
    }

    protected void deleteTopic(final String topic) {
        log.info("Delete topic: {}", topic);
        try (final AdminClient adminClient = AdminClient.create(this.kafkaProperties)) {
            adminClient.deleteTopics(List.of(topic));
        }
    }

    protected void resetSchemaRegistry(final String topic) {
        log.info("Reset topic: {}", topic);
        try {
            final Collection<String> allSubjects = this.client.getAllSubjects();
            final String keySubject = topic + "-key";
            if (allSubjects.contains(keySubject)) {
                this.client.deleteSubject(keySubject);
                log.info("Cleaned key schema of topic {}", topic);
            } else {
                log.info("No key schema for topic {} available", topic);
            }
            final String valueSubject = topic + "-value";
            if (allSubjects.contains(valueSubject)) {
                this.client.deleteSubject(valueSubject);
                log.info("Cleaned value schema of topic {}", topic);
            } else {
                log.info("No value schema for topic {} available", topic);
            }
        } catch (final IOException | RestClientException e) {
            throw new RuntimeException("Could not reset schema registry for topic " + topic, e);
        }
    }


}