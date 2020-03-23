package com.bakdata.common_kafka_streams.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.test_applications.ComplexTopologyApplication;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologyInformationTest {

    private KafkaStreamsApplication app = null;
    private TopologyInformation topologyInformation = null;

    @BeforeEach
    void setup() {
        this.app = new ComplexTopologyApplication();
        this.app.setInputTopics(List.of("input", "input2"));
        this.app.setOutputTopic("output");
        this.topologyInformation = new TopologyInformation(this.app.createTopology(), this.app.getUniqueAppId());
    }

    @Test
    void shouldReturnAllExternalSinkTopics() {
        assertThat(this.topologyInformation.getExternalSinkTopics())
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC,
                        this.app.getOutputTopic());
    }

    @Test
    void shouldReturnAllExternalSourceTopics() {
        assertThat(this.topologyInformation.getExternalSourceTopics())
                .hasSize(2)
                .containsAll(this.app.getInputTopics())
                .doesNotContain(ComplexTopologyApplication.THROUGH_TOPIC);
    }

    @Test
    void shouldNotReturnInputTopics() {
        assertThat(this.topologyInformation.getExternalSinkTopics())
                .doesNotContainAnyElementsOf(this.app.getInputTopics());
    }

    @Test
    void shouldReturnAllInternalTopics() {
        assertThat(this.topologyInformation.getInternalTopics())
                .hasSize(3)
                .allMatch(topic -> topic.startsWith(this.app.getUniqueAppId()))
                .allMatch(topic -> topic.contains("-KSTREAM-"))
                .allMatch(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"));
    }

}