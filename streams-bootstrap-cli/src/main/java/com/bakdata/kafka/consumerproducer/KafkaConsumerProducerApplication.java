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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.bakdata.kafka.KafkaApplication;
import com.bakdata.kafka.StringListConverter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.UseDefaultConverter;


/**
 * <p>The base class for creating Kafka ConsumerProducer applications.</p>
 * This class provides the following configuration options in addition to those provided by {@link KafkaApplication}:
 * <ul>
 *     <li>{@link #inputTopics}</li>
 *     <li>{@link #inputPattern}</li>
 *     <li>{@link #errorTopic}</li>
 *     <li>{@link #labeledInputTopics}</li>
 *     <li>{@link #labeledInputPatterns}</li>
 *     <li>{@link #outputTopic}</li>
 *     <li>{@link #labeledOutputTopics}</li>
 *     <li>{@link #applicationId}</li>
 * </ul>
 * To implement your Kafka ConsumerProducer application inherit from this class and add your custom options.  Run it by
 * creating an instance of your class and calling {@link #startApplication(String[])} from your main.
 *
 * @param <T> type of {@link ConsumerProducerApp} created by this application
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka ConsumerProducer application.")
public abstract class KafkaConsumerProducerApplication<T extends ConsumerProducerApp> extends
        KafkaApplication<ConsumerProducerRunner, ConsumerProducerCleanUpRunner, ConsumerProducerExecutionOptions,
                ExecutableConsumerProducerApp<T>, ConfiguredConsumerProducerApp<T>, ConsumerProducerTopicConfig, T,
                ConsumerProducerAppConfiguration> {
    // TODO mixin inputtopicconfig - separate error, separate input, separate output, what about applicationid? maybe consumeroptions?
    @CommandLine.Option(names = "--input-topics", description = "Input topics", split = ",")
    private List<String> inputTopics = emptyList();
    @CommandLine.Option(names = "--input-pattern", description = "Input pattern")
    private Pattern inputPattern;
    @CommandLine.Option(names = "--error-topic", description = "Error topic")
    private String errorTopic;
    @CommandLine.Option(names = "--labeled-input-topics", split = ",", description = "Additional labeled input topics",
            converter = {UseDefaultConverter.class, StringListConverter.class})
    private Map<String, List<String>> labeledInputTopics = emptyMap();
    @CommandLine.Option(names = "--labeled-input-patterns", split = ",",
            description = "Additional labeled input patterns")
    private Map<String, Pattern> labeledInputPatterns = emptyMap();
    @CommandLine.Option(names = "--output-topic", description = "Output topic")
    private String outputTopic;
    @CommandLine.Option(names = "--labeled-output-topics", split = ",",
            description = "Additional labeled output topics")
    private Map<String, String> labeledOutputTopics = emptyMap();
    @CommandLine.Option(names = "--application-id",
            description =
                    "Unique application ID to use for Kafka ConsumerProducer. Can also be provided by implementing "
                            + "ConsumerProducerApp#getUniqueAppId()")
    private String applicationId;

    /**
     * Reset the Kafka ConsumerProducer application. Additionally, delete the consumer group and all output topics
     * associated with the Kafka ConsumerProducer application.
     */
    @Command(description = "Reset the Kafka ConsumerProducer application. Additionally, delete the consumer group and "
            + "all output topics associated with the Kafka ConsumerProducer application.")
    @Override
    public void clean() {
        super.clean();
    }

    /**
     * Clear all state stores and consumer group offsets associated with the Kafka ConsumerProducer application.
     */
    @Command(description = "Clear all state stores, consumer group offsets, and internal topics associated with the "
            + "Kafka Streams application.")
    public void reset() {
        this.prepareClean();
        try (final CleanableApp<ConsumerProducerCleanUpRunner> app = this.createCleanableApp()) {
            final ConsumerProducerCleanUpRunner runner = app.getCleanUpRunner();
            runner.reset();
        }
    }

    @Override
    public final Optional<ConsumerProducerExecutionOptions> createExecutionOptions() {
        return Optional.empty();
    }

    @Override
    public final ConsumerProducerTopicConfig createTopicConfig() {
        return ConsumerProducerTopicConfig.builder()
                .inputTopics(this.inputTopics)
                .labeledInputTopics(this.labeledInputTopics)
                .inputPattern(this.inputPattern)
                .labeledInputPatterns(this.labeledInputPatterns)
                .outputTopic(this.getOutputTopic())
                .labeledOutputTopics(this.getLabeledOutputTopics())
                .errorTopic(this.errorTopic)
                .build();
    }

    @Override
    public final ConfiguredConsumerProducerApp<T> createConfiguredApp(final T app,
            final ConsumerProducerAppConfiguration configuration) {
        return new ConfiguredConsumerProducerApp<>(app, configuration);
    }

    @Override
    public ConsumerProducerAppConfiguration createConfiguration(final ConsumerProducerTopicConfig topics) {
        return new ConsumerProducerAppConfiguration(topics, this.applicationId);
    }

}
