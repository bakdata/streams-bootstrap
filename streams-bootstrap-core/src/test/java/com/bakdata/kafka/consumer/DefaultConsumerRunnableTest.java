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

import static com.bakdata.kafka.consumer.TestHelper.assertContent;
import static com.bakdata.kafka.consumer.TestHelper.createExecutableApp;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.ConsumerGroupsClient.ConsumerGroupClient;
import com.bakdata.kafka.consumer.apps.CustomProcessorConsumer;
import com.bakdata.kafka.consumer.apps.StringConsumer;
import java.util.List;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class DefaultConsumerRunnableTest extends KafkaTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    static ConfiguredConsumerApp<ConsumerApp> createStringApplication() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        return new ConfiguredConsumerApp<>(new StringConsumer(), new ConsumerAppConfiguration(topics));
    }

    static ConfiguredConsumerApp<ConsumerApp> createCustomProcessorConsumer(
            final Consumer<ConsumerRecords<String, String>> recordProcessor) {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        return new ConfiguredConsumerApp<>(new CustomProcessorConsumer(recordProcessor),
                new ConsumerAppConfiguration(topics));
    }

    @Test
    void shouldRunProcessAndShutdownGracefully() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            testClient.send()
                    .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));
            final List<KeyValue<String, String>> expectedValues =
                    List.of(new KeyValue<>("blub", "blub"),
                            new KeyValue<>("bla", "bla"),
                            new KeyValue<>("blub", "blub")
                    );

            final StringConsumer stringConsumer = (StringConsumer) app.app();

            runAsync(runner);
            awaitActive(executableApp);
            assertContent(this.softly, stringConsumer.getConsumedRecords(), expectedValues,
                    "Contains all elements after first run");
        }
    }

    @Test
    void shouldCommitOffsets() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createStringApplication();
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));

            runAsync(runner);
            awaitActive(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueAppId());

                this.softly.assertThat(consumerGroupClient.listOffsets().values())
                        .as("Offset for topic")
                        .isEmpty();

                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to(app.getTopics().getInputTopics().get(0), List.of(
                                new SimpleProducerRecord<>("blub", "blub")
                        ));
                awaitProcessing(executableApp);

                this.softly.assertThat(consumerGroupClient.listOffsets().values().stream().findAny())
                        .map(OffsetAndMetadata::offset)
                        .as("Offset for topic")
                        .hasValue(1L);
            }
        }
    }

    @Test
    void shouldNotCommitAndTerminateWhenProcessorThrowsException() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createCustomProcessorConsumer(
                processor -> {throw new RuntimeException("Error while processing records");});
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));

            runAsync(runner);
            awaitActive(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final ConsumerGroupClient consumerGroupClient =
                        adminClient.consumerGroups().group(app.getUniqueAppId());

                this.softly.assertThat(consumerGroupClient.listOffsets().values())
                        .as("Offset for topic")
                        .isEmpty();

                testClient.send()
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .to(app.getTopics().getInputTopics().get(0), List.of(
                                new SimpleProducerRecord<>("blub", "blub")
                        ));
                awaitClosed(executableApp);

                this.softly.assertThat(consumerGroupClient.listOffsets().values())
                        .as("Offset for topic")
                        .isEmpty();
            }
        }
    }

    @Test
    void shouldThrowExceptionWhenAlreadyRunning() {
        try (final ConfiguredConsumerApp<ConsumerApp> app = createCustomProcessorConsumer(
                processor -> {
                    throw new RuntimeException("Error while processing records");
                });
                final ExecutableConsumerApp<ConsumerApp> executableApp = createExecutableApp(app, this.createConfig());
                final ConsumerRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));

            runAsync(runner);
            awaitActive(executableApp);

            assertThatThrownBy(runner::run).isInstanceOf(ConsumerApplicationException.class)
                    .hasMessage("Consumer already running");
        }
    }
}
