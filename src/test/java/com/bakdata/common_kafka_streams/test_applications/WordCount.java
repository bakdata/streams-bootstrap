/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.common_kafka_streams.test_applications;

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import picocli.CommandLine.Command;

@NoArgsConstructor
public class WordCount extends KafkaStreamsApplication {
    public static void main(final String[] args) {
        startApplication(new WordCount(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final KStream<String, String> textLines = builder.stream(this.inputTopic);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();

        wordCounts.toStream().to(this.outputTopic, Produced.with(stringSerde, longSerde));
    }

    @Override
    public String getUniqueAppId() {
        return this.getClass().getSimpleName() + "-" + this.getInputTopic() + "-" + this.getOutputTopic();
    }

    public Properties getKafkaProperties() {
        final Properties kafkaConfig = super.getKafkaProperties();
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kafkaConfig;
    }
}
