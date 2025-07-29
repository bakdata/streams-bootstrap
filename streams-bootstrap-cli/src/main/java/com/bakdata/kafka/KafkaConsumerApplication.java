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

package com.bakdata.kafka;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import com.bakdata.kafka.consumer.ConfiguredConsumerApp;
import com.bakdata.kafka.consumer.ConsumerApp;
import com.bakdata.kafka.consumer.ConsumerCleanUpRunner;
import com.bakdata.kafka.consumer.ConsumerExecutionOptions;
import com.bakdata.kafka.consumer.ConsumerRunner;
import com.bakdata.kafka.consumer.ConsumerTopicConfig;
import com.bakdata.kafka.consumer.ExecutableConsumerApp;
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
 * <p>The base class for creating Kafka Consumer applications.</p>
 * This class provides all configuration options provided by {@link KafkaApplication}. To implement your Kafka Consumer
 * application inherit from this class and add your custom options. Run it by calling
 * {@link #startApplication(KafkaApplication, String[])} with an instance of your class from your main.
 *
 * @param <T> type of {@link ConsumerApp} created by this application
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka Consumer application")
public abstract class KafkaConsumerApplication<T extends ConsumerApp> extends
        KafkaApplication<ConsumerRunner, ConsumerCleanUpRunner, ConsumerExecutionOptions, ExecutableConsumerApp<T>,
                ConfiguredConsumerApp<T>, ConsumerTopicConfig, T> {
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
    @CommandLine.Option(names = "--volatile-group-instance-id", arity = "0..1",
            description = "Whether the group instance id is volatile, i.e., it will change on a Streams shutdown.")
    private boolean volatileGroupInstanceId;
    @CommandLine.Option(names = "--application-id",
            description = "Unique application ID to use for Kafka Streams. Can also be provided by implementing "
                    + "StreamsApp#getUniqueAppId()")
    private String applicationId;
    // TODO what about output topics?

    @Override
    public final Optional<ConsumerExecutionOptions> createExecutionOptions() {
        return Optional.empty();
    }

    @Override
    public final ConsumerTopicConfig createTopicConfig() {
        return ConsumerTopicConfig.builder()
                .inputTopics(this.inputTopics)
                .labeledInputTopics(this.labeledInputTopics)
                .inputPattern(this.inputPattern)
                .labeledInputPatterns(this.labeledInputPatterns)
                .build();
    }

    @Override
    public final ConfiguredConsumerApp<T> createConfiguredApp(final T app, final ConsumerTopicConfig topics) {
        return new ConfiguredConsumerApp<>(app, topics);
    }
}
