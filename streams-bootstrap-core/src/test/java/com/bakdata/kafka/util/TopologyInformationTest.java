/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ConfiguredStreamsApp;
import com.bakdata.kafka.KafkaEndpointConfig;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.test_applications.ComplexTopologyApplication;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TableJoined;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopologyInformationTest {

    private StreamsApp app = null;
    private StreamsTopicConfig topics;
    private TopologyInformation topologyInformation = null;

    @BeforeEach
    void setup() {
        this.app = new ComplexTopologyApplication();
        this.topics = StreamsTopicConfig.builder()
                .inputTopics(List.of("input", "input2"))
                .outputTopic("output")
                .build();
        final AppConfiguration<StreamsTopicConfig> configuration = new AppConfiguration<>(this.topics);
        final ConfiguredStreamsApp<StreamsApp> configuredApp = new ConfiguredStreamsApp<>(this.app, configuration);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(
                KafkaEndpointConfig.builder()
                        .bootstrapServers("localhost:9092")
                        .build());
        this.topologyInformation =
                new TopologyInformation(configuredApp.createTopology(kafkaProperties),
                        this.app.getUniqueAppId(this.topics));
    }

    @Test
    void shouldReturnAllExternalSinkTopics() {
        assertThat(this.topologyInformation.getExternalSinkTopics())
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC,
                        this.topics.getOutputTopic());
    }

    @Test
    void shouldReturnAllExternalSourceTopics() {
        assertThat(this.topologyInformation.getExternalSourceTopics(List.of()))
                .hasSize(2)
                .containsAll(this.topics.getInputTopics())
                .doesNotContain(ComplexTopologyApplication.THROUGH_TOPIC);
    }

    @Test
    void shouldReturnAllIntermediateTopics() {
        assertThat(this.topologyInformation.getIntermediateTopics(List.of()))
                .hasSize(1)
                .containsExactly(ComplexTopologyApplication.THROUGH_TOPIC)
                .doesNotContainAnyElementsOf(this.topics.getInputTopics());
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
                .doesNotContainAnyElementsOf(this.topics.getInputTopics());
    }

    @Test
    void shouldReturnAllInternalTopics() {
        assertThat(this.topologyInformation.getInternalTopics())
                .hasSize(3)
                .allMatch(topic -> topic.contains("-KSTREAM-") && topic.startsWith(this.app.getUniqueAppId(this.topics))
                        || topic.startsWith("KSTREAM-"))
                .allMatch(topic -> topic.endsWith("-changelog") || topic.endsWith("-repartition"));
    }

    @Test
    void shouldReturnAllPseudoInternalTopicsForForeignKeyJoin() {
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
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-0000000006-topic-vh",
                        "id-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-0000000014-topic"
                );
    }

    @Test
    void shouldReturnAllPseudoInternalTopicsForNamedForeignKeyJoin() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KTable<String, Object> t1 = streamsBuilder.table("t1");
        final KTable<Integer, Object> t2 = streamsBuilder.table("t2");
        t1
                .leftJoin(t2, ignored -> 1, (o1, o2) -> o1, TableJoined.as("join"))
                .toStream()
                .to("output");
        final TopologyInformation topologyInformation = new TopologyInformation(streamsBuilder.build(), "id");
        assertThat(topologyInformation.getInternalTopics())
                .contains(
                        "id-join-subscription-registration-topic",
                        "id-join-subscription-registration-topic-fk",
                        "id-join-subscription-registration-topic-pk",
                        "id-join-subscription-registration-topic-vh",
                        "id-join-subscription-response-topic"
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
