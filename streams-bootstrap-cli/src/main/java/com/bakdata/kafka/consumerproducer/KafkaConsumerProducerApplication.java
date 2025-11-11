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

import com.bakdata.kafka.KafkaApplication;
import com.bakdata.kafka.mixin.ConsumerOptions;
import com.bakdata.kafka.mixin.ErrorOptions;
import com.bakdata.kafka.mixin.InputOptions;
import com.bakdata.kafka.mixin.OutputOptions;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;


/**
 * <p>The base class for creating Kafka ConsumerProducer applications.</p>
 * This class provides the following configuration options in addition to those provided by {@link KafkaApplication}:
 * <ul>
 *     <li>{@link #getInputTopics()}</li>
 *     <li>{@link #getInputPattern()}</li>
 *     <li>{@link #getErrorTopic()}</li>
 *     <li>{@link #getLabeledInputTopics()}</li>
 *     <li>{@link #getLabeledInputPatterns()}</li>
 *     <li>{@link #getOutputTopic()}</li>
 *     <li>{@link #getLabeledOutputTopics()}</li>
 *     <li>{@link #isVolatileGroupInstanceId()}</li>
 *     <li>{@link #getApplicationId()} ()}</li>
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
    @Mixin
    @Delegate
    private InputOptions inputOptions = new InputOptions();
    @Mixin
    @Delegate
    private OutputOptions outputOptions = new OutputOptions();
    @Mixin
    @Delegate
    private ErrorOptions errorOptions = new ErrorOptions();
    @Mixin
    @Delegate
    private ConsumerOptions consumerOptions = new ConsumerOptions();

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
                .inputTopics(this.getInputTopics())
                .labeledInputTopics(this.getLabeledInputTopics())
                .inputPattern(this.getInputPattern())
                .labeledInputPatterns(this.getLabeledInputPatterns())
                .outputTopic(this.getOutputTopic())
                .labeledOutputTopics(this.getLabeledOutputTopics())
                .errorTopic(this.getErrorTopic())
                .build();
    }

    @Override
    public final ConfiguredConsumerProducerApp<T> createConfiguredApp(final T app,
            final ConsumerProducerAppConfiguration configuration) {
        return new ConfiguredConsumerProducerApp<>(app, configuration);
    }

    @Override
    public ConsumerProducerAppConfiguration createConfiguration(final ConsumerProducerTopicConfig topics) {
        return new ConsumerProducerAppConfiguration(topics, this.getUniqueIdentifier());
    }

}
