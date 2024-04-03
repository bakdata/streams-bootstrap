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

package com.bakdata.kafka;

import com.bakdata.kafka.util.ImprovedAdminClient;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

@Builder
@Value
public class TopologyBuilder {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    @NonNull StreamsTopicConfig topics;
    @NonNull Map<String, Object> kafkaProperties;
    boolean cleanUp;

    public <K, V> KStream<K, V> streamInput(final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(), consumed);
    }

    public <K, V> KStream<K, V> streamInput() {
        return this.streamsBuilder.stream(this.topics.getInputTopics());
    }

    public <K, V> KStream<K, V> streamInput(final String role, final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(role), consumed);
    }

    public <K, V> KStream<K, V> streamInput(final String role) {
        return this.streamsBuilder.stream(this.topics.getInputTopics(role));
    }

    public <K, V> KStream<K, V> streamInputPattern(final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(), consumed);
    }

    public <K, V> KStream<K, V> streamInputPattern() {
        return this.streamsBuilder.stream(this.topics.getInputPattern());
    }

    public <K, V> KStream<K, V> streamInputPattern(final String role, final Consumed<K, V> consumed) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(role), consumed);
    }

    public <K, V> KStream<K, V> streamInputPattern(final String role) {
        return this.streamsBuilder.stream(this.topics.getInputPattern(role));
    }

    public Map<String, Object> getKafkaProperties() {
        return Collections.unmodifiableMap(this.kafkaProperties);
    }

    public ImprovedAdminClient createAdminClient() {
        return ImprovedAdminClient.create(this.kafkaProperties);
    }

    public Topology build() {
        return this.streamsBuilder.build();
    }
}
