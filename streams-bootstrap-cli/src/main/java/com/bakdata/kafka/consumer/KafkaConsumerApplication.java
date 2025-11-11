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

import com.bakdata.kafka.KafkaApplication;
import com.bakdata.kafka.mixin.ConsumerOptions;
import com.bakdata.kafka.mixin.InputOptions;
import java.time.Duration;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;


/**
 * <p>The base class for creating Kafka Consumer applications.</p>
 * This class provides the following configuration options in addition to those provided by {@link KafkaApplication}:
 * <ul>
 *     <li>{@link #getInputTopics()}</li>
 *     <li>{@link #getInputPattern()}</li>
 *     <li>{@link #getLabeledInputTopics()}</li>
 *     <li>{@link #getLabeledInputPatterns()}</li>
 *     <li>{@link #isVolatileGroupInstanceId()}</li>
 *     <li>{@link #getUniqueIdentifier()} Unique Group Id</li>
 * </ul>
 * To implement your Kafka Consumer application inherit from this class and add your custom options.  Run it by
 * creating an instance of your class and calling {@link #startApplication(String[])} from your main.
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
                ConfiguredConsumerApp<T>, ConsumerTopicConfig, T, ConsumerAppConfiguration> {
    @Mixin
    @Delegate
    private InputOptions inputOptions = new InputOptions();
    @Mixin
    @Delegate
    private ConsumerOptions consumerOptions = new ConsumerOptions();
    @Option(names = {"--poll-timeout"},
            description = "The maximum time to block in the consumer poll loop. Examples: 'PT0.1S', 'PT2S', 'PT1M'.",
            defaultValue = "PT0.1S")
    private Duration pollTimeout = Duration.ofMillis(100);

    /**
     * Reset the Kafka Consumer application. Additionally, delete the consumer group.
     */
    @Command(description = "Reset the Kafka Consumer application. Additionally, delete the consumer group.")
    @Override
    public void clean() {
        super.clean();
    }

    /**
     * Clear consumer group offsets of the Kafka Consumer application.
     */
    @Command(description = "Clear consumer group offsets of the Kafka Consumer application")
    public void reset() {
        this.prepareClean();
        try (final CleanableApp<ConsumerCleanUpRunner> app = this.createCleanableApp()) {
            final ConsumerCleanUpRunner runner = app.getCleanUpRunner();
            runner.reset();
        }
    }

    @Override
    public final Optional<ConsumerExecutionOptions> createExecutionOptions() {
        final ConsumerExecutionOptions executionOptions = ConsumerExecutionOptions.builder()
                .volatileGroupInstanceId(this.isVolatileGroupInstanceId())
                .onStart(this::onConsumerStart)
                .pollTimeout(this.getPollTimeout())
                .build();
        return Optional.of(executionOptions);
    }

    @Override
    public final ConsumerTopicConfig createTopicConfig() {
        return ConsumerTopicConfig.builder()
                .inputTopics(this.getInputTopics())
                .labeledInputTopics(this.getLabeledInputTopics())
                .inputPattern(this.getInputPattern())
                .labeledInputPatterns(this.getLabeledInputPatterns())
                .build();
    }

    @Override
    public final ConfiguredConsumerApp<T> createConfiguredApp(final T app,
            final ConsumerAppConfiguration configuration) {
        return new ConfiguredConsumerApp<>(app, configuration);
    }

    @Override
    public ConsumerAppConfiguration createConfiguration(final ConsumerTopicConfig topics) {
        return new ConsumerAppConfiguration(topics, this.getUniqueIdentifier());
    }

    /**
     * Called after starting Kafka Consumer
     *
     * @param runningConsumer running {@link ConsumerRunnable} instance along with its {@link ConsumerConfig}
     */
    protected void onConsumerStart(final RunningConsumer runningConsumer) {
        // do nothing by default
    }
}
