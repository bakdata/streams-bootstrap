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

package com.bakdata.kafka.consumerproducer;

import static com.bakdata.kafka.consumerproducer.ConsumerProducerCleanUpRunnerTest.createStringConsumerProducer;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.ConsumerGroupVerifier;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ConsumerProducerRunnerTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldRunApp() {
        final RuntimeConfiguration configuration = this.createConfig();
        try (final ConfiguredConsumerProducerApp<ConsumerProducerApp> app = createStringConsumerProducer();
                final ConsumerProducerRunner runner = app.withRuntimeConfiguration(configuration)
                        .createRunner()) {
            final ConsumerGroupVerifier consumerGroupVerifier = ConsumerGroupVerifier.verify(app, configuration);
            final KafkaTestClient testClient = this.newTestClient();
            final String inputTopic = app.getTopics().getInputTopics().get(0);
            final String outputTopic = app.getTopics().getOutputTopic();
            testClient.createTopic(inputTopic);
            testClient.createTopic(outputTopic);
            runAsync(runner);
            awaitProcessing(consumerGroupVerifier);

            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(inputTopic, List.of(new SimpleProducerRecord<>("foo", "bar")));

            this.softly.assertThat(testClient.read()
                            .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                            .from(outputTopic, POLL_TIMEOUT))
                    .hasSize(1);
        }
    }

}
