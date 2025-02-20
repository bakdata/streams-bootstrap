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

package com.bakdata.kafka.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;

class TopologyInformationTest {

    private static final String THROUGH_TOPIC = "through-topic";
    private static final String OUTPUT_TOPIC = "output";
    private static final List<String> INPUT_TOPICS = List.of("input1", "input2");

    private static Topology buildComplexTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(INPUT_TOPICS);

        input.to(THROUGH_TOPIC);
        final KStream<String, String> through = builder.stream(THROUGH_TOPIC);
        final KTable<Windowed<String>, String> reduce = through
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(5L)))
                .reduce((a, b) -> a);

        reduce.toStream()
                .map((k, v) -> KeyValue.pair(v, ""))
                .groupByKey()
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    @Test
    void shouldReturnAllExternalSinkTopics() {
        final TopologyInformation topologyInformation =
                new TopologyInformation(buildComplexTopology(), "id");
        assertThat(topologyInformation.getExternalSinkTopics())
                .containsExactly(THROUGH_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void shouldReturnAllExternalSourceTopics() {
        final TopologyInformation topologyInformation =
                new TopologyInformation(buildComplexTopology(), "id");
        assertThat(topologyInformation.getExternalSourceTopics(List.of()))
                .hasSize(2)
                .containsAll(INPUT_TOPICS)
                .doesNotContain(THROUGH_TOPIC);
    }

    @Test
    void shouldReturnAllIntermediateTopics() {
        final TopologyInformation topologyInformation =
                new TopologyInformation(buildComplexTopology(), "id");
        assertThat(topologyInformation.getIntermediateTopics(List.of()))
                .hasSize(1)
                .containsExactly(THROUGH_TOPIC)
                .doesNotContainAnyElementsOf(INPUT_TOPICS);
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
        final TopologyInformation topologyInformation =
                new TopologyInformation(buildComplexTopology(), "id");
        assertThat(topologyInformation.getExternalSinkTopics())
                .doesNotContainAnyElementsOf(INPUT_TOPICS);
    }

    @Test
    void shouldReturnAllInternalTopics() {
        final TopologyInformation topologyInformation =
                new TopologyInformation(buildComplexTopology(), "id");
        assertThat(topologyInformation.getInternalTopics())
                .hasSize(3)
                .allMatch(topic -> topic.contains("-KSTREAM-") && topic.startsWith("id")
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
