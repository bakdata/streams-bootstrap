package com.bakdata.common_kafka_streams;

import static com.bakdata.common_kafka_streams.KafkaStreamsApplication.RESET_SLEEP_MS;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kafka.tools.StreamsResetter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Sink;


@Slf4j
public class CleanUpRunner {
    private final Topology topology;
    private final String streamsId;
    private final Properties kafkaProperties;
    private final SchemaRegistryClient client;
    private final List<String> inputTopics;
    private final String brokers;
    private final boolean deleteOutputTopic;
    private final KafkaStreams streams;

    public CleanUpRunner(final KafkaStreamsApplication application) {
        this.topology = application.createTopology();
        this.streamsId = application.getUniqueAppId();
        this.kafkaProperties = application.getKafkaProperties();
        this.client = new CachedSchemaRegistryClient(application.getSchemaRegistryUrl(), 100);
        this.inputTopics = application.getInputTopics();
        this.brokers = application.getBrokers();
        this.deleteOutputTopic = application.isDeleteOutputTopic();
        this.streams = application.getStreams();
    }

    public void run() {
        runResetter(String.join(",", this.inputTopics), this.brokers, this.streamsId);
        if (this.deleteOutputTopic) {
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
        final List<Node> nodes = this.getNodes(this.topology);

        this.getInternalTopics(nodes).forEach(this::resetSchemaRegistry);

        final List<String> externalTopics = this.getExternalTopics(nodes);
        externalTopics.forEach(this::deleteTopic);
        externalTopics.forEach(this::resetSchemaRegistry);
    }

    protected void deleteTopic(final String topic) {
        log.info("Delete topic: {}", topic);
        // the streams resetter is responsible for deleting internal topics
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

    private List<Node> getNodes(final Topology topology) {
        return topology.describe().subtopologies()
                .stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .collect(Collectors.toList());
    }

    private List<String> getExternalTopics(final Collection<Node> nodes) {
        return this.getAllSinks(nodes)
                .filter(this::isExternalTopic)
                .collect(Collectors.toList());
    }

    private List<String> getInternalTopics(final Collection<Node> nodes) {
        final Stream<String> internalSinks = this.getInternalSinks(nodes);
        final Stream<String> backingTopics = this.getBackingTopics(nodes);

        return Stream.concat(internalSinks, backingTopics).collect(Collectors.toList());
    }

    private Stream<String> getInternalSinks(final Collection<Node> nodes) {
        return this.getAllSinks(nodes)
                .filter(this::isInternalTopic)
                .map(topic -> String.format("%s-%s", this.streamsId, topic));
    }

    private Stream<String> getAllSinks(final Collection<Node> nodes) {
        return nodes.stream()
                .filter(node -> node instanceof Sink)
                .map(node -> ((Sink) node))
                .map(Sink::topic);
    }

    private Stream<String> getBackingTopics(final Collection<Node> nodes) {
        return nodes.stream()
                .filter(node -> node instanceof Processor)
                .map(node -> ((Processor) node))
                .flatMap(processor -> processor.stores().stream())
                .map(store -> String.format("%s-%s-changelog", this.streamsId, store));
    }

    private boolean isInternalTopic(final String topic) {
        return topic.startsWith("KSTREAM-") || topic.startsWith("KTABLE-");
    }

    private boolean isExternalTopic(final String topic) {
        return !isInternalTopic(topic);
    }
}