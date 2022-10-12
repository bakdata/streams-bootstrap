/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
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
import java.util.regex.Pattern;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
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
        assertThat(this.topologyInformation.getExternalSourceTopics(List.of()))
                .hasSize(2)
                .containsAll(this.app.getInputTopics())
                .doesNotContain(ComplexTopologyApplication.THROUGH_TOPIC);
    }

    @Test
    void shouldReturnAllIntermediateTopics() {
        assertThat(this.topologyInformation.getIntermediateTopics(List.of()))
                .hasSize(1)
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC)
                .doesNotContainAnyElementsOf(this.app.getInputTopics());
    }

    @Test
    void shouldReturnImplicitRepartitionTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("input")
                // select key to force repartition
                .selectKey((k, v) -> v)
                .groupByKey()
                .count(Materialized.as("counts"))
                .toStream()
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getIntermediateTopics(List.of()))
                .isEmpty();
        assertThat(topologyInformation.getInternalTopics())
                .contains("id-counts-repartition");
    }

    @Test
    void shouldReturnRepartitionTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("input")
                .repartition(Repartitioned.as("rep"))
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getIntermediateTopics(List.of()))
                .isEmpty();
        assertThat(topologyInformation.getInternalTopics())
                .hasSize(1)
                .containsExactly("id-rep-repartition");
    }

    @Test
    void shouldNotReturnFakeRepartitionTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final String throughTopic = "rep-repartition";
        streamsBuilder.stream("input")
                .to(throughTopic);
        streamsBuilder.stream(throughTopic)
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getIntermediateTopics(List.of()))
                .hasSize(1)
                .containsExactly("rep-repartition");
        assertThat(topologyInformation.getInternalTopics()).isEmpty();
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
                .allMatch(topic -> topic.contains("-KSTREAM-") && topic.startsWith(this.app.getUniqueAppId())
                        || topic.startsWith("KSTREAM-"))
                .allMatch(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"));
    }

    @Test
    void shouldReturnAllPseudoInternalTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KTable<String, Object> t1 = streamsBuilder.table("t1");
        final KTable<Integer, Object> t2 = streamsBuilder.table("t2");
        t1
                .leftJoin(t2, ignored -> 1, (o1, o2) -> o1)
                .toStream()
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getInternalTopics())
                .contains(
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic",
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-fk",
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-pk",
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-vh"
                );
    }

    @Test
    void shouldResolvePatternTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, Object> stream = streamsBuilder.stream(Pattern.compile(".*-topic"));
        stream.to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(
                topologyInformation.getExternalSourceTopics(List.of("foo", "foo-topic", "foo-topic-bar", "bar-topic")))
                .hasSize(2)
                .containsExactly("foo-topic", "bar-topic");
        assertThat(topologyInformation.getExternalSourceTopics(List.of())).isEmpty();
    }

    @Test
    void shouldResolveIntermediatePatternTopics() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, Object> stream = streamsBuilder.stream("input");
        stream.to("through-topic");
        final KStream<String, Object> through = streamsBuilder.stream(Pattern.compile(".*-topic"));
        through.to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getIntermediateTopics(
                List.of("foo", "foo-topic", "foo-topic-bar", "through-topic")))
                .hasSize(1)
                .containsExactly("through-topic");
        assertThat(topologyInformation.getIntermediateTopics(List.of())).isEmpty();
        assertThat(topologyInformation.getExternalSourceTopics(
                List.of("foo", "foo-topic", "foo-topic-bar", "through-topic")))
                .hasSize(2)
                .containsExactly("input", "foo-topic");
    }

}
