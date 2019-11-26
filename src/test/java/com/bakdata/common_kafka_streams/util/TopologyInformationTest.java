package com.bakdata.common_kafka_streams.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.test_applications.ComplexTopologyApplication;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologyInformationTest {

    private KafkaStreamsApplication application = null;

    @BeforeEach
    void setup() {
        this.application = new ComplexTopologyApplication();
        this.application.setInputTopics(List.of("input"));
        this.application.setOutputTopic("output");
    }

    @Test
    void shouldReturnAllExternalSinkTopics() {
        this.setup();

        final TopologyInformation topologyInformation =
                new TopologyInformation(this.application.createTopology(), this.application.getUniqueAppId());
        assertThat(topologyInformation.getExternalSinkTopics())
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC,
                        this.application.getOutputTopic());
    }

    @Test
    void shouldNotReturnInputTopics() {
        this.setup();

        final TopologyInformation topologyInformation =
                new TopologyInformation(this.application.createTopology(), this.application.getUniqueAppId());
        assertThat(topologyInformation.getExternalSinkTopics())
                .doesNotContain(this.application.getInputTopic());
    }

    @Test
    void shouldReturnAllInternalTopics() {
        final TopologyInformation topologyInformation =
                new TopologyInformation(this.application.createTopology(), this.application.getUniqueAppId());
        assertThat(topologyInformation.getInternalTopics())
                .hasSize(3)
                .allMatch(topic -> topic.startsWith(this.application.getUniqueAppId()))
                .allMatch(topic -> topic.contains("-KSTREAM-"))
                .allMatch(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"));
    }

}