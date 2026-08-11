/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

package com.bakdata.kafka.streams;


import static com.bakdata.kafka.TestHelper.clean;
import static com.bakdata.kafka.TestHelper.reset;
import static com.bakdata.kafka.TestHelper.run;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.CleanUpException;
import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestHelper;
import com.bakdata.kafka.admin.AdminClientX;
import com.bakdata.kafka.admin.StreamsGroupsClient;
import com.bakdata.kafka.admin.TopicsClient;
import com.bakdata.kafka.streams.apps.Mirror;
import com.bakdata.kafka.streams.apps.WordCount;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(SoftAssertionsExtension.class)
class StreamsCleanUpRunnerStreamsGroupTest extends KafkaTest {
    private static final Map<String, String> STREAMS_GROUP_CONFIG =
            Map.of(StreamsConfig.GROUP_PROTOCOL_CONFIG, "streams");

    @InjectSoftAssertions
    private SoftAssertions softly;
    @TempDir
    private Path stateDir;

    private static ConfiguredStreamsApp<StreamsApp> createWordCountApplication() {
        final StreamsApp app = new WordCount();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("word_input"))
                .outputTopic("word_output")
                .build()));
    }

    private static ConfiguredStreamsApp<StreamsApp> createMirrorApplication() {
        final StreamsApp app = new Mirror();
        return new ConfiguredStreamsApp<>(app, new StreamsAppConfiguration(StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build()));
    }

    private RuntimeConfiguration createStreamsGroupConfig() {
        return super.createConfig().with(STREAMS_GROUP_CONFIG);
    }

    @Test
    void shouldDeleteTopicWithStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app,
                        this.createStreamsGroupConfig())) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX admin = testClient.admin()) {
                final TopicsClient topics = admin.topics();
                this.softly.assertThat(topics.topic(app.getTopics().getOutputTopic()).exists())
                        .as("Output topic is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldDeleteStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app)) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "WordCount contains all elements after first run");

            try (final AdminClientX adminClient = testClient.admin()) {
                final StreamsGroupsClient groups = adminClient.streamsGroups();
                this.softly.assertThat(groups.group(app.getUniqueAppId()).exists())
                        .as("Streams group exists")
                        .isTrue();
            }

            awaitClosed(executableApp);
            clean(executableApp);

            try (final AdminClientX adminClient = testClient.admin()) {
                final StreamsGroupsClient groups = adminClient.streamsGroups();
                this.softly.assertThat(groups.group(app.getUniqueAppId()).exists())
                        .as("Streams group is deleted")
                        .isFalse();
            }
        }
    }

    @Test
    void shouldDeleteStateWithStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app)) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "blub"),
                            new SimpleProducerRecord<>(null, "bla"),
                            new SimpleProducerRecord<>(null, "blub")
                    ));

            final List<KeyValue<String, Long>> expectedValues =
                    List.of(new KeyValue<>("blub", 1L),
                            new KeyValue<>("bla", 1L),
                            new KeyValue<>("blub", 2L)
                    );

            run(executableApp);
            this.assertContent(app.getTopics().getOutputTopic(), expectedValues,
                    "All entries appear once in the output topic after the 1st run");

            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            final List<KeyValue<String, Long>> entriesTwice = expectedValues.stream()
                    .flatMap(entry -> Stream.of(entry, entry))
                    .toList();
            this.assertContent(app.getTopics().getOutputTopic(), entriesTwice,
                    "All entries appear twice in the output topic after the 2nd run");
        }
    }

    @Test
    void shouldReprocessAlreadySeenRecordsWithStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createWordCountApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app)) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getOutputTopic());
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(app.getTopics().getInputTopics().get(0), List.of(
                            new SimpleProducerRecord<>(null, "a"),
                            new SimpleProducerRecord<>(null, "b"),
                            new SimpleProducerRecord<>(null, "c")
                    ));

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);
            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 3);

            // Wait until all stream applications are completely stopped before triggering cleanup
            awaitClosed(executableApp);
            reset(executableApp);

            run(executableApp);
            this.assertSize(app.getTopics().getOutputTopic(), 6);
        }
    }

    @Test
    void shouldNotThrowExceptionOnMissingInputTopicWithStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app)) {
            this.softly.assertThatCode(() -> clean(executableApp)).doesNotThrowAnyException();
        }
    }

    @Test
    void shouldNotThrowExceptionOnResetOfActiveStreamsGroup() {
        try (final ConfiguredStreamsApp<StreamsApp> app = createMirrorApplication();
                final ExecutableStreamsApp<StreamsApp> executableApp = this.createExecutableApp(app);
                final StreamsRunner runner = executableApp.createRunner()) {
            final KafkaTestClient testClient = this.newTestClient();
            testClient.createTopic(app.getTopics().getInputTopics().get(0));
            runAsync(runner);
            // Wait until stream application has consumed all data
            awaitActive(executableApp);
            // Unlike the classic StreamsResetter, StreamsGroupCommand does not fail on active groups
            this.softly.assertThatCode(() -> reset(executableApp)).doesNotThrowAnyException();
        }
    }

    private ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app) {
        return TestHelper.createExecutableApp(app, this.createStreamsGroupConfig(), this.stateDir);
    }

    private ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app,
            final RuntimeConfiguration configuration) {
        return TestHelper.createExecutableApp(app, configuration, this.stateDir);
    }

    private List<KeyValue<String, Long>> readOutputTopic(final String outputTopic) {
        final List<ConsumerRecord<String, Long>> records = this.newTestClient().read()
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new LongDeserializer())
                .from(outputTopic, POLL_TIMEOUT);
        return records.stream()
                .map(TestHelper::toKeyValue)
                .toList();
    }

    private void assertContent(final String outputTopic,
            final Iterable<? extends KeyValue<String, Long>> expectedValues, final String description) {
        final List<KeyValue<String, Long>> output = this.readOutputTopic(outputTopic);
        this.softly.assertThat(output)
                .as(description)
                .containsExactlyInAnyOrderElementsOf(expectedValues);
    }

    private void assertSize(final String outputTopic, final int expectedMessageCount) {
        final List<KeyValue<String, Long>> records = this.readOutputTopic(outputTopic);
        this.softly.assertThat(records).hasSize(expectedMessageCount);
    }

}


