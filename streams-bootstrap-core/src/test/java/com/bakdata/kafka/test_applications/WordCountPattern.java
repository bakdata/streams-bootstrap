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

package com.bakdata.kafka.test_applications;

import com.bakdata.kafka.SerdeConfig;
import com.bakdata.kafka.StreamsApp;
import com.bakdata.kafka.StreamsTopicConfig;
import com.bakdata.kafka.TopologyBuilder;
import java.util.Arrays;
import java.util.regex.Pattern;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

@NoArgsConstructor
public class WordCountPattern implements StreamsApp {

    @Override
    public void buildTopology(final TopologyBuilder builder) {
        final KStream<String, String> textLines = builder.streamInputPattern();

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count(Materialized.as("counts"));

        final Serde<Long> longValueSerde = Serdes.Long();
        wordCounts.toStream().to(builder.getTopics().getOutputTopic(), Produced.valueSerde(longValueSerde));
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
