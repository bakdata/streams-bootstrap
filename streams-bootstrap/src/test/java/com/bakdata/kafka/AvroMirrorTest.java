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

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.test_applications.MirrorWithNonDefaultSerde;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class AvroMirrorTest {
    private final ConfiguredStreamsApp<MirrorWithNonDefaultSerde> app = createApp();
    @RegisterExtension
    final TestTopologyExtension<String, TestRecord> testTopology =
            StreamsBootstrapTopologyFactory.createTopologyExtensionWithSchemaRegistry(this.app);

    private static ConfiguredStreamsApp<MirrorWithNonDefaultSerde> createApp() {
        final StreamsAppConfiguration configuration = StreamsAppConfiguration.builder()
                .topics(StreamsTopicConfig.builder()
                        .inputTopics(List.of("input"))
                        .outputTopic("output")
                        .build())
                .build();
        return configuration.configure(new MirrorWithNonDefaultSerde());
    }

    @Test
    void shouldMirror() {
        final Serde<TestRecord> valueSerde = MirrorWithNonDefaultSerde.getValueSerde(
                this.testTopology.getStreamsConfig().originals());
        final TestRecord record = TestRecord.newBuilder()
                .setContent("bar")
                .build();
        this.testTopology.input()
                .withValueSerde(valueSerde)
                .add("foo", record);

        this.testTopology.streamOutput()
                .withValueSerde(valueSerde)
                .expectNextRecord().hasKey("foo").hasValue(record)
                .expectNoMoreRecord();
    }
}
