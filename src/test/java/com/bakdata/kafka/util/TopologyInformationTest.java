/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import java.util.List;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
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
    void shouldReturnAllIntermediateTopics() {
        assertThat(this.topologyInformation.getIntermediateTopics())
                .hasSize(1)
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC)
                .doesNotContainAnyElementsOf(this.app.getInputTopics());
    }

    @Test
    void shouldNotReturnRepartitionTopicAsIntermediateTopic() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("input")
                // select key to force repartition
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Materialized.as("counts"))
                .toStream()
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getIntermediateTopics())
                .isEmpty();
    }

    @Test
    void shouldNotReturnInputTopics() {
        assertThat(this.topologyInformation.getExternalSinkTopics())
                .doesNotContainAnyElementsOf(this.app.getInputTopics());
    }

    @Test
    void shouldReturnAllInternalTopics() {
        assertThat(this.topologyInformation.getInternalTopics())
                .hasSize(5)
                .allMatch(topic -> topic.contains("-KSTREAM-") && topic.startsWith(this.app.getUniqueAppId())
                        || topic.startsWith("KSTREAM-"))
                .allMatch(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"));
    }

}