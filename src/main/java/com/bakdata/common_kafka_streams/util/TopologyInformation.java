package com.bakdata.common_kafka_streams.util;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription.Node;
import org.apache.kafka.streams.TopologyDescription.Processor;
import org.apache.kafka.streams.TopologyDescription.Sink;

public class TopologyInformation {
    private final String streamsId;
    private final Collection<Node> nodes;

    public TopologyInformation(final Topology topology, final String streamsId) {
        this.nodes = getNodes(topology);
        this.streamsId = streamsId;
    }

    public List<String> getInternalTopics() {
        final Stream<String> internalSinks = this.getInternalSinks(this.nodes);
        final Stream<String> backingTopics = this.getBackingTopics(this.nodes);

        return Stream.concat(internalSinks, backingTopics).collect(Collectors.toList());
    }

    public List<String> getExternalSinkTopics() {
        return this.getAllSinks(this.nodes)
                .filter(this::isExternalTopic)
                .collect(Collectors.toList());
    }

    private static List<Node> getNodes(final Topology topology) {
        return topology.describe().subtopologies()
                .stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .collect(Collectors.toList());
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
        return !this.isInternalTopic(topic);
    }
}
