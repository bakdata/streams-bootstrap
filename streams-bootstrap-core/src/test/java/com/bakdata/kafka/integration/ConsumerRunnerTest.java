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

import static com.bakdata.kafka.integration.ConsumerCleanUpRunnerTest.createStringApplication;

import com.bakdata.kafka.ConfiguredConsumerApp;
import com.bakdata.kafka.ConsumerApp;
import com.bakdata.kafka.ConsumerRunner;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.test_applications.StringConsumer;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldRunApp() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ConsumerRunner runner = app.withRuntimeConfiguration(this.createConfigWithoutSchemaRegistry())
                        .createRunner()) {

            new Thread(runner).start();

            final SimpleProducerRecord<String, String> simpleProducerRecord = new SimpleProducerRecord<>("foo", "bar");
            this.writeInputTopic(app.getTopics().getInputTopics().get(0), simpleProducerRecord);

            final StringConsumer stringConsumer = (StringConsumer) app.getApp();

            Awaitility.await()
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> {
                        final List<KeyValue<String, String>> consumedRecords = stringConsumer.getConsumedRecords()
                                .stream()
                                .map(StreamsCleanUpRunnerTest::toKeyValue)
                                .toList();
                        this.softly.assertThat(consumedRecords)
                                .containsExactlyInAnyOrderElementsOf(List.of(new KeyValue<>("foo", "bar")));
                    });
        }
    }

    private void writeInputTopic(final String inputTopic, final SimpleProducerRecord<String, String> producerRecord) {
        final KafkaTestClient testClient = this.newTestClient();
        testClient.send()
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .to(inputTopic, (Iterable)List.of(producerRecord));
    }

}
