/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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
import java.util.Properties;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@NoArgsConstructor
public class MirrorWithNonDefaultSerde extends KafkaStreamsApplication {
    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final Serde<TestRecord> valueSerde = this.getValueSerde();
        final KStream<String, TestRecord> input =
                builder.stream(this.getInputTopics(), Consumed.with(null, valueSerde));
        input.to(this.getOutputTopic(), Produced.valueSerde(valueSerde));
    }

    public Serde<TestRecord> getValueSerde() {
        final Serde<TestRecord> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(new StreamsConfig(this.getKafkaProperties()).originals(), false);
        return valueSerde;
    }

    @Override
    public String getUniqueAppId() {
        return this.getClass().getSimpleName() + "-" + this.getOutputTopic();
    }

    @Override
    public Properties createKafkaProperties() {
        final Properties kafkaConfig = super.createKafkaProperties();
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaConfig;
    }
}
