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

package com.bakdata.kafka.consumer;

import static com.bakdata.kafka.consumer.ConsumerCleanUpRunnerTest.createStringApplication;
import static com.bakdata.kafka.consumer.TestHelper.createExecutableApp;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.consumer.apps.StringConsumer;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldRunApp() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {

            runAsync(runner);
            awaitActive(executableApp);

            final SimpleProducerRecord<String, String> simpleProducerRecord = new SimpleProducerRecord<>("foo", "bar");
            this.writeInputTopic(app.getTopics().getInputTopics().get(0), simpleProducerRecord);

            final StringConsumer stringConsumer = (StringConsumer) app.app();

            awaitProcessing(executableApp);

            final List<KeyValue<String, String>> consumedRecords = stringConsumer.getConsumedRecords()
                    .stream()
                    .map(TestHelper::toKeyValue)
                    .toList();
            this.softly.assertThat(consumedRecords)
                    .containsExactlyInAnyOrderElementsOf(List.of(new KeyValue<>("foo", "bar")));
        }
    }

    private void writeInputTopic(final String inputTopic, final SimpleProducerRecord<String, String> producerRecord) {
        final KafkaTestClient testClient = this.newTestClient();
        testClient.send()
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                .to(inputTopic, (Iterable) List.of(producerRecord));
    }

}
