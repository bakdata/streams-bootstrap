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

package com.bakdata.kafka.integration;


import static com.bakdata.kafka.TestUtil.newKafkaCluster;
import static net.mguenther.kafka.junit.Wait.delay;

import com.bakdata.kafka.CloseFlagApp;
import com.bakdata.kafka.KafkaStreamsApplication;
import com.bakdata.kafka.SimpleKafkaStreamsApplication;
import com.bakdata.kafka.test_applications.WordCount;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@Slf4j
@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class StreamsCleanUpTest {
    private static final int TIMEOUT_SECONDS = 10;
    private final EmbeddedKafkaCluster kafkaCluster = newKafkaCluster();
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
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @BeforeEach
    void setup() throws InterruptedException {
        this.kafkaCluster.start();
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        this.kafkaCluster.stop();
    }

    @Test
    void shouldClean() throws InterruptedException {
        try (final KafkaStreamsApplication<?> app = this.createWordCountApplication()) {
            final SendValuesTransactional<String> sendRequest =
                    SendValuesTransactional.inTransaction(app.getInputTopics().get(0),
                            List.of("blub", "bla", "blub")).useDefaults();
            this.kafkaCluster.send(sendRequest);

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            app.clean();

            this.softly.assertThat(this.kafkaCluster.exists(app.getOutputTopic()))
                    .as("Output topic is deleted")
                    .isFalse();

            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldReset() throws InterruptedException {
        try (final KafkaStreamsApplication<?> app = this.createWordCountApplication()) {
            final SendValuesTransactional<String> sendRequest =
                    SendValuesTransactional.inTransaction(app.getInputTopics().get(0),
                            List.of("blub", "bla", "blub")).useDefaults();
            this.kafkaCluster.send(sendRequest);

            final List<KeyValue<String, Long>> expectedValues = List.of(
                    new KeyValue<>("blub", 1L),
                    new KeyValue<>("bla", 1L),
                    new KeyValue<>("blub", 2L)
            );
            this.runAndAssertContent(expectedValues, "All entries are once in the input topic after the 1st run", app);

            // Wait until all stream application are completely stopped before triggering cleanup
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            app.reset();

            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .collect(Collectors.toList());
            this.runAndAssertContent(entriesTwice, "All entries are twice in the input topic after the 2nd run", app);
        }
    }

    @Test
    void shouldCallClose() throws InterruptedException {
        try (final CloseFlagApp app = this.createCloseFlagApplication()) {
            this.kafkaCluster.createTopic(TopicConfig.withName(app.getInputTopics().get(0)).useDefaults());
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            this.softly.assertThat(app.isClosed()).isFalse();
            this.softly.assertThat(app.isAppClosed()).isFalse();
            // if we don't run the app, the coordinator will be unavailable
            runAppAndClose(app);
            this.softly.assertThat(app.isAppClosed()).isTrue();
            app.setAppClosed(false);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            app.clean();
            this.softly.assertThat(app.isAppClosed()).isTrue();
            app.setAppClosed(false);
            delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) throws InterruptedException {
        final ReadKeyValues<String, Long> readRequest = ReadKeyValues.from(outputTopic, Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class).build();
        return this.kafkaCluster.read(readRequest);
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
        application.setBootstrapServers(this.kafkaCluster.getBrokerList());
        application.setKafkaConfig(Map.of(
                StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
        ));
        return application;
    }

}
