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


import com.bakdata.kafka.CloseFlagApp;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.test_applications.WordCount;
import com.bakdata.kafka.util.AdminClientX;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
class StreamsCleanUpTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    @TempDir
    private Path stateDir;

    private static CloseFlagApp createCloseFlagApplication() {
        final CloseFlagApp app = new CloseFlagApp();
        app.setInputTopics(List.of("input"));
        app.setOutputTopic("output");
        return app;
    }

    private static KafkaStreamsApplication<?> createWordCountApplication() {
        final KafkaStreamsApplication<?> application = new SimpleKafkaStreamsApplication<>(WordCount::new);
        application.setOutputTopic("word_output");
        application.setInputTopics(List.of("word_input"));
        return application;
    }

    @Test
    void shouldClean() {
        try (final KafkaStreamsApplication<?> app = createWordCountApplication()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(app.createExecutableApp());
            this.clean(app);

            try (final AdminClientX admin = testClient.admin()) {
                this.softly.assertThat(admin.getTopicClient().exists(app.getOutputTopic()))
                        .as("Output topic is deleted")
                        .isFalse();
            }

            testClient.createTopic(app.getOutputTopic());
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldReset() {
        try (final KafkaStreamsApplication<?> app = createWordCountApplication()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(app.createExecutableApp());
            this.reset(app);

            try (final AdminClientX admin = testClient.admin()) {
                this.softly.assertThat(admin.getTopicClient().exists(app.getOutputTopic()))
                        .as("Output topic exists")
                        .isTrue();
            }

            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .toList();
            this.runAndAssertContent(entriesTwice, "All entries are twice in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldCallClose() {
        try (final CloseFlagApp app = createCloseFlagApplication()) {
            this.newTestClient().createTopic(app.getInputTopics().get(0));
            this.softly.assertThat(app.isClosed()).isFalse();
            this.softly.assertThat(app.isAppClosed()).isFalse();
            this.clean(app);
            this.softly.assertThat(app.isAppClosed()).isTrue();
            app.setAppClosed(false);
            this.reset(app);
            this.softly.assertThat(app.isAppClosed()).isTrue();
        }
    }

    private void clean(final KafkaStreamsApplication<?> app) {
        this.createTestRunner().clean(app);
    }

    private void reset(final KafkaStreamsApplication<?> app) {
        this.createTestRunner().reset(app);
    }

    private void runAppAndClose(final KafkaStreamsApplication<?> app) {
        this.runApp(app);
        app.stop();
    }

    private void runApp(final KafkaStreamsApplication<?> app) {
        this.createTestRunner().run(app);
        // Wait until stream application has consumed all data
        awaitProcessing(app.createExecutableApp());
    }

    private TestApplicationRunner createTestRunner() {
        return TestApplicationRunner.create(this.getBootstrapServers())
                .withStateDir(this.stateDir)
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, Long>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new LongDeserializer())
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(consumerRecord -> new KeyValue<>(consumerRecord.key(), consumerRecord.value()))
                .toList();
    }

    private void runAndAssertContent(final Iterable<? extends KeyValue<String, Long>> expectedValues,
            final String description, final KafkaStreamsApplication<?> app) {
        this.runAppAndClose(app);

        final List<KeyValue<String, Long>> output = this.readOutputTopic(app.getOutputTopic());
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

}
