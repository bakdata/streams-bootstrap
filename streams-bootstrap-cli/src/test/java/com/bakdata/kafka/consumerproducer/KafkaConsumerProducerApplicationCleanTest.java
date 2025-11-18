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


import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.consumerproducer.apps.CloseFlagApp;
import com.bakdata.kafka.consumerproducer.apps.Mirror;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
class KafkaConsumerProducerApplicationCleanTest extends KafkaTest {
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

    private static KafkaConsumerProducerApplication<?> createMirrorApplication() {
        final KafkaConsumerProducerApplication<?> application =
                new SimpleKafkaConsumerProducerApplication<>(Mirror::new);
        application.setOutputTopic("output");
        application.setInputTopics(List.of("input"));
        return application;
    }

    @Test
    void shouldClean() {
        try (final KafkaConsumerProducerApplication<?> app = createMirrorApplication()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues = List.of(
                    new KeyValue<>("blub", "blub"),
                    new KeyValue<>("bla", "bla"),
                    new KeyValue<>("blub", "blub")
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all applications are completely stopped before triggering cleanup
            awaitClosed(app.createExecutableApp());
            this.clean(app);

            try (final AdminClientX admin = testClient.admin()) {
                this.softly.assertThat(admin.topics().topic(app.getOutputTopic()).exists())
                        .as("Output topic is deleted")
                        .isFalse();
            }

            testClient.createTopic(app.getOutputTopic());
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldReset() {
        try (final KafkaConsumerProducerApplication<?> app = createMirrorApplication()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>("blub", "blub"),
                            new SimpleProducerRecord<>("bla", "bla"),
                            new SimpleProducerRecord<>("blub", "blub")
                    ));

            final List<KeyValue<String, String>> expectedValues = List.of(
                    new KeyValue<>("blub", "blub"),
                    new KeyValue<>("bla", "bla"),
                    new KeyValue<>("blub", "blub")
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all applications are completely stopped before triggering cleanup
            awaitClosed(app.createExecutableApp());
            this.reset(app);

            try (final AdminClientX admin = testClient.admin()) {
                this.softly.assertThat(admin.topics().topic(app.getOutputTopic()).exists())
                        .as("Output topic exists")
                        .isTrue();
            }

            final List<KeyValue<String, String>> entriesTwice = expectedValues.stream()
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

    private void clean(final KafkaConsumerProducerApplication<?> app) {
        this.createTestRunner().clean(app);
    }

    private void reset(final KafkaConsumerProducerApplication<?> app) {
        this.createTestRunner().reset(app);
    }

    private void runAppAndClose(final KafkaConsumerProducerApplication<?> app) {
        this.runApp(app);
        app.stop();
    }

    private void runApp(final KafkaConsumerProducerApplication<?> app) {
        this.createTestRunner().run(app);
        // Wait until the application has consumed all data
        awaitProcessing(app.createExecutableApp());
    }

    private TestApplicationRunner createTestRunner() {
        return TestApplicationRunner.create(this.getBootstrapServers())
                .withStateDir(this.stateDir)
                .withNoStateStoreCaching()
                .withSessionTimeout(SESSION_TIMEOUT);
    }

    private List<KeyValue<String, String>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, String>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(consumerRecord -> new KeyValue<>(consumerRecord.key(), consumerRecord.value()))
                .toList();
    }

    private void runAndAssertContent(final Iterable<? extends KeyValue<String, String>> expectedValues,
            final String description, final KafkaConsumerProducerApplication<?> app) {
        this.runAppAndClose(app);

        final List<KeyValue<String, String>> output = this.readOutputTopic(app.getOutputTopic());
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

}
