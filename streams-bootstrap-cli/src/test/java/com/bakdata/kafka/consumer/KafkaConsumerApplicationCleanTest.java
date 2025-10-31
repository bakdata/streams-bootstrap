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


import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.consumer.apps.CloseFlagApp;
import com.bakdata.kafka.consumer.apps.StringConsumer;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
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
class KafkaConsumerApplicationCleanTest extends KafkaTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    @TempDir
    private Path stateDir;

    private static CloseFlagApp createCloseFlagApplication() {
        final CloseFlagApp app = new CloseFlagApp();
        app.setInputTopics(List.of("input"));
        return app;
    }

    private void runAppAndClose(final KafkaConsumerApplication<?> app) {
        this.runApp(app);
        app.stop();
    }

    private void runApp(final KafkaConsumerApplication<?> app) {
        this.createTestRunner().run(app);
        // Wait until stream application has consumed all data
        awaitProcessing(app.createExecutableApp());
    }

    @Test
    void shouldClean() {
        final String input = "input";
        try (final StringConsumer stringConsumer = new StringConsumer();
                final KafkaConsumerApplication<?> application = new SimpleKafkaConsumerApplication<>(
                        () -> stringConsumer)) {
            application.setInputTopics(List.of(input));
            final KafkaTestClient testClient = this.newTestClient();
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(input, List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues = List.of(
                    new KeyValue<>("blub", "blub"),
                    new KeyValue<>("bla", "bla"),
                    new KeyValue<>("blub", "blub")
            );

            this.runAppAndClose(application);
            this.assertContent(expectedValues, "All entries are read from the input topic after the 1st run",
                    stringConsumer);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(application.createExecutableApp());
            this.clean(application);

            this.runAppAndClose(application);
            final List<KeyValue<String, String>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .toList();
            this.assertContent(entriesTwice, "All entries are read twice from the input topic after the 2nd run",
                    stringConsumer);
        }
    }

    @Test
    void shouldReset() {
        final String input = "input";
        try (final StringConsumer stringConsumer = new StringConsumer();
                final KafkaConsumerApplication<?> application = new SimpleKafkaConsumerApplication<>(
                        () -> stringConsumer)) {
            final KafkaTestClient testClient = this.newTestClient();
            application.setInputTopics(List.of(input));
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(input, List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues = List.of(
                    new KeyValue<>("blub", "blub"),
                    new KeyValue<>("bla", "bla"),
                    new KeyValue<>("blub", "blub")
            );

            this.runAppAndClose(application);
            this.assertContent(expectedValues, "All entries are read once from the input topic after the 1st run",
                    stringConsumer);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(application.createExecutableApp());
            this.reset(application);

            this.runAppAndClose(application);
            final List<KeyValue<String, String>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .toList();
            this.assertContent(entriesTwice, "All entries are read twice from the input topic after the 2nd run",
                    stringConsumer);
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

    private void clean(final KafkaConsumerApplication<?> app) {
        this.createTestRunner().clean(app);
    }

    private void reset(final KafkaConsumerApplication<?> app) {
        this.createTestRunner().reset(app);
    }

    private TestApplicationRunner createTestRunner() {
        return TestApplicationRunner.create(this.getBootstrapServers())
                .withStateDir(this.stateDir)
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
    }

    private void assertContent(final Iterable<? extends KeyValue<String, String>> expectedValues,
            final String description, final StringConsumer consumer) {
        this.softly.assertThat(consumer.getConsumedRecords())
                .map(consumerRecord -> new KeyValue<>(consumerRecord.key(), consumerRecord.value()))
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

}
