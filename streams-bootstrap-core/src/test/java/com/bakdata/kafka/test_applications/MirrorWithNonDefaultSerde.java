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

package com.bakdata.kafka.test_applications;

import com.bakdata.kafka.ConfiguredConsumed;
import com.bakdata.kafka.ConfiguredProduced;
import com.bakdata.kafka.ImprovedKStream;
import com.bakdata.kafka.Preconfigured;
import com.bakdata.kafka.SerdeConfig;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.TopologyBuilder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

@NoArgsConstructor
public class MirrorWithNonDefaultSerde implements StreamsApp {

    public static Preconfigured<Serde<TestRecord>> newKeySerde() {
        return Preconfigured.create(new SpecificAvroSerde<>());
    }

    public static Preconfigured<Serde<TestRecord>> newValueSerde() {
        return Preconfigured.create(new SpecificAvroSerde<>());
    }

    @Override
    public void buildTopology(final TopologyBuilder builder) {
        final Preconfigured<Serde<TestRecord>> valueSerde = newValueSerde();
        final Preconfigured<Serde<TestRecord>> keySerde = newKeySerde();
        final ImprovedKStream<TestRecord, TestRecord> input =
                builder.streamInput(ConfiguredConsumed.with(keySerde, valueSerde));
        input.toOutputTopic(ConfiguredProduced.with(keySerde, valueSerde));
    }

    @Override
    public String getUniqueAppId(final StreamsTopicConfig topics) {
        return this.getClass().getSimpleName() + "-" + topics.getOutputTopic();
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(StringSerde.class, StringSerde.class);
    }
}
