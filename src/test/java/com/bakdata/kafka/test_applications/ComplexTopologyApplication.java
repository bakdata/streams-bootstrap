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

package com.bakdata.kafka.test_applications;

import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.TestRecord;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class ComplexTopologyApplication extends KafkaStreamsApplication {

    public static final String THROUGH_TOPIC = "through-topic";

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final KStream<String, TestRecord> input = builder.stream(this.getInputTopics());

        input.to(THROUGH_TOPIC);
        final KStream<String, TestRecord> through = builder.stream(THROUGH_TOPIC);
        final KTable<Windowed<String>, TestRecord> reduce = through
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(5L)))
                .reduce((a, b) -> a);

        reduce.toStream()
                .map((k, v) -> KeyValue.pair(v.getContent(), v))
                .groupByKey()
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to(this.getOutputTopic(), Produced.with(Serdes.String(), Serdes.Long()));
    }

    @Override
    public String getUniqueAppId() {
        return this.getClass().getSimpleName() + "-" + this.getOutputTopic();
    }

    @Override
    public Properties createKafkaProperties() {
        final Properties kafkaConfig = super.createKafkaProperties();
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        return kafkaConfig;
    }
}
