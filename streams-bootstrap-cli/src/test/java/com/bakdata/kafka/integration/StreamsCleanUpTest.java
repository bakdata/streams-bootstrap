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


import static com.bakdata.kafka.KafkaContainerHelper.DEFAULT_TOPIC_SETTINGS;
import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static java.util.Collections.emptyMap;

import com.bakdata.kafka.CloseFlagApp;
import com.bakdata.kafka.KafkaContainerHelper;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.test_applications.WordCount;
import com.bakdata.kafka.util.ImprovedAdminClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@Testcontainers
@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsCleanUpTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    @Container
    private final ConfluentKafkaContainer kafkaCluster = newKafkaCluster();
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static void runAppAndClose(final KafkaStreamsApplication<?> app) throws InterruptedException {
        runApp(app);
        app.stop();
    }

    private static void runApp(final KafkaStreamsApplication<?> app) throws InterruptedException {
        // run in Thread because the application blocks indefinitely
        new Thread(app).start();
        // Wait until stream application has consumed all data
        Thread.sleep(TIMEOUT.toMillis());
    }

    @Test
    void shouldClean() throws InterruptedException {
        try (final KafkaStreamsApplication<?> app = this.createWordCountApplication()) {
            final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
            try (final ImprovedAdminClient admin = new KafkaContainerHelper(this.kafkaCluster).admin()) {
                admin.getTopicClient().createTopic(app.getOutputTopic(), DEFAULT_TOPIC_SETTINGS, emptyMap());
            }
            kafkaContainerHelper.send()
                    .to(app.getInputTopics().get(0), List.of(
                            new KeyValue<>(null, "blub"),
                            new KeyValue<>(null, "bla"),
                            new KeyValue<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream application are completely stopped before triggering cleanup
            Thread.sleep(TIMEOUT.toMillis());
            app.clean();

            try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
                this.softly.assertThat(admin.getTopicClient().exists(app.getOutputTopic()))
                        .as("Output topic is deleted")
                        .isFalse();
            }

            try (final ImprovedAdminClient admin = new KafkaContainerHelper(this.kafkaCluster).admin()) {
                admin.getTopicClient().createTopic(app.getOutputTopic(), DEFAULT_TOPIC_SETTINGS, emptyMap());
            }
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldReset() throws InterruptedException {
        try (final KafkaStreamsApplication<?> app = this.createWordCountApplication()) {
            final KafkaContainerHelper kafkaContainerHelper = new KafkaContainerHelper(this.kafkaCluster);
            try (final ImprovedAdminClient admin = new KafkaContainerHelper(this.kafkaCluster).admin()) {
                admin.getTopicClient().createTopic(app.getOutputTopic(), DEFAULT_TOPIC_SETTINGS, emptyMap());
            }
            kafkaContainerHelper.send()
                    .to(app.getInputTopics().get(0), List.of(
                            new KeyValue<>(null, "blub"),
                            new KeyValue<>(null, "bla"),
                            new KeyValue<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream application are completely stopped before triggering cleanup
            Thread.sleep(TIMEOUT.toMillis());
            app.reset();

            try (final ImprovedAdminClient admin = kafkaContainerHelper.admin()) {
                this.softly.assertThat(admin.getTopicClient().exists(app.getOutputTopic()))
                        .as("Output topic exists")
                        .isTrue();
            }

            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .collect(Collectors.toList());
            this.runAndAssertContent(entriesTwice, "All entries are twice in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldCallClose() throws InterruptedException {
        try (final CloseFlagApp app = this.createCloseFlagApplication()) {
            try (final ImprovedAdminClient admin = new KafkaContainerHelper(this.kafkaCluster).admin()) {
                admin.getTopicClient().createTopic(app.getInputTopics().get(0), DEFAULT_TOPIC_SETTINGS, emptyMap());
            }
            Thread.sleep(TIMEOUT.toMillis());
            this.softly.assertThat(app.isClosed()).isFalse();
            this.softly.assertThat(app.isAppClosed()).isFalse();
            app.clean();
            this.softly.assertThat(app.isAppClosed()).isTrue();
            app.setAppClosed(false);
            Thread.sleep(TIMEOUT.toMillis());
            app.reset();
            this.softly.assertThat(app.isAppClosed()).isTrue();
        }
    }

    private CloseFlagApp createCloseFlagApplication() {
        final CloseFlagApp app = new CloseFlagApp();
        app.setInputTopics(List.of("input"));
        app.setOutputTopic("output");
        return this.configure(app);
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, Long>> records = new KafkaContainerHelper(this.kafkaCluster).read()
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .from(outputTopic, TIMEOUT);
        return records.stream()
                .map(record -> new KeyValue<>(record.key(), record.value()))
                .collect(Collectors.toList());
    }

    private void runAndAssertContent(final Iterable<? extends KeyValue<String, Long>> expectedValues,
            final String description, final KafkaStreamsApplication<?> app)
            throws InterruptedException {
        runAppAndClose(app);

        final List<KeyValue<String, Long>> output = this.readOutputTopic(app.getOutputTopic());
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private KafkaStreamsApplication<?> createWordCountApplication() {
        final KafkaStreamsApplication<?> application = new SimpleKafkaStreamsApplication<>(WordCount::new);
        application.setOutputTopic("word_output");
        application.setInputTopics(List.of("word_input"));
        return this.configure(application);
    }

    private <T extends KafkaStreamsApplication<?>> T configure(final T application) {
        application.setBootstrapServers(this.kafkaCluster.getBootstrapServers());
        application.setKafkaConfig(Map.of(
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
        ));
        return application;
    }

}
