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

package com.bakdata.kafka.streams.apps;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.SerdeConfig;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import org.apache.kafka.common.serialization.Serdes.DoubleSerde;

public abstract class DoubleApp implements StreamsApp {

    @Override
    public String getUniqueAppId(final StreamsAppConfiguration configuration) {
        return "my-app";
    }

    @Override
    public SerdeConfig defaultSerializationConfig() {
        return new SerdeConfig(DoubleSerde.class, DoubleSerde.class);
    }

    public TestTopology<Double, Double> startApp(final StreamsTopicConfig topicConfig) {
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(this, new StreamsAppConfiguration(topicConfig));
        return TestHelper.startApp(configuredApp);
    }

    public TestTopology<Double, Double> startApp() {
        return this.startApp(StreamsTopicConfig.builder().build());
    }
}
