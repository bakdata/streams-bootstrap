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

package com.bakdata.kafka;

import java.util.Map;
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
 * To implement your Kafka Producer application inherit from this class and add your custom options. Run it by
 * calling {@link #startApplication(KafkaApplication, String[])} with a instance of your class from your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
@Command(description = "Run a Kafka Producer application")
public abstract class KafkaProducerApplication
        extends KafkaApplication<ProducerRunner, ProducerCleanUpRunner, ProducerExecutionOptions, ProducerApp> {

    /**
     * Create a new {@code ProducerApp} that will be configured and executed according to this application.
     */
    @Override
    public abstract ProducerApp createApp(boolean cleanUp);

    /**
     * Delete all output topics associated with the Kafka Producer application.
     */
    @Command(description = "Delete all output topics associated with the Kafka Producer application.")
    @Override
    public void clean() {
        super.clean();
    }

    /**
     * @see ProducerRunner#run()
     */
    @Override
    public void run() {
        super.run();
    }

    @Override
    public final ProducerAppConfiguration createConfiguration() {
        final ProducerTopicConfig topics = this.createTopicConfig();
        final Map<String, String> kafkaConfig = this.getKafkaConfig();
        return ProducerAppConfiguration.builder()
                .topics(topics)
                .kafkaConfig(kafkaConfig)
                .build();
    }

    @Override
    public final Optional<ProducerExecutionOptions> createExecutionOptions() {
        return Optional.empty();
    }

    public final ProducerTopicConfig createTopicConfig() {
        return ProducerTopicConfig.builder()
                .outputTopic(this.getOutputTopic())
                .extraOutputTopics(this.getExtraOutputTopics())
                .build();
    }
}
