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

package com.bakdata.kafka.integration;

import static com.bakdata.kafka.integration.ProducerCleanUpRunnerTest.createStringApplication;

import com.bakdata.kafka.ConfiguredProducerApp;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.ProducerApp;
import com.bakdata.kafka.ProducerRunner;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ProducerRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldRunApp() {
        try (final ConfiguredProducerApp<ProducerApp> app = createStringApplication();
                final ProducerRunner runner = app.withEndpoint(this.createEndpointWithoutSchemaRegistry())
                        .createRunner()) {
            runner.run();

            final List<KeyValue<String, String>> output = this.readOutputTopic(app.getTopics().getOutputTopic());
            this.softly.assertThat(output)
                    .containsExactlyInAnyOrderElementsOf(List.of(new KeyValue<>("foo", "bar")));
        }
    }

    private List<KeyValue<String, String>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, String>> records = this.newTestClient().read()
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(StreamsCleanUpRunnerTest::toKeyValue)
                .collect(Collectors.toList());
    }

}
