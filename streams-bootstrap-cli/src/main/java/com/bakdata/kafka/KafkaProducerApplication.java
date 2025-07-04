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

import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;


/**
 * <p>The base class for creating Kafka Producer applications.</p>
 * This class provides all configuration options provided by {@link KafkaApplication}.
 * To implement your Kafka Producer application inherit from this class and add your custom options.  Run it by
 * creating an instance of your class and calling {@link #startApplication(String[])} from your main.
 *
 * @param <T> type of {@link ProducerApp} created by this application
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka Producer application")
public abstract class KafkaProducerApplication<T extends ProducerApp> extends
        KafkaApplication<ProducerRunner, ProducerCleanUpRunner, ProducerExecutionOptions, ExecutableProducerApp<T>,
                ConfiguredProducerApp<T>, ProducerTopicConfig, T, ProducerAppConfiguration> {

    /**
     * Delete all output topics associated with the Kafka Producer application.
     */
    @Command(description = "Delete all output topics associated with the Kafka Producer application.")
    @Override
    public void clean() {
        super.clean();
    }

    @Override
    public final Optional<ProducerExecutionOptions> createExecutionOptions() {
        return Optional.empty();
    }

    @Override
    public final ProducerTopicConfig createTopicConfig() {
        return ProducerTopicConfig.builder()
                .outputTopic(this.getOutputTopic())
                .labeledOutputTopics(this.getLabeledOutputTopics())
                .build();
    }

    @Override
    public final ConfiguredProducerApp<T> createConfiguredApp(final T app,
            final ProducerAppConfiguration configuration) {
        return new ConfiguredProducerApp<>(app, configuration);
    }

    @Override
    public ProducerAppConfiguration createConfiguration(final ProducerTopicConfig topics) {
        return new ProducerAppConfiguration(topics);
    }
}
