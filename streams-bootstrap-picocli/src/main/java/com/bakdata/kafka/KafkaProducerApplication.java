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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


/**
 * <p>The base class of the entry point of a producer application.</p>
 * This class provides common configuration options, e.g., {@link #brokers}, for producer applications. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments
 * {@link EnvironmentArgumentsParser}. To implement your producer application inherit from this class and add your
 * custom options. Call {@link #startApplication(KafkaApplication, String[])} with a fresh instance of your class from
 * your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
public abstract class KafkaProducerApplication extends KafkaApplication {

    @Override
    public void run() {
        final ProducerRunner runner = this.createRunner();
        runner.run();
    }

    @Override
    public void clean() {
        final ProducerCleanUpRunner cleanUpRunner = this.createCleanUpRunner();
        cleanUpRunner.clean();
    }

    public ConfiguredProducerApp<ProducerApp> createConfiguredApp() {
        final ProducerApp producerApp = this.createApp();
        final ProducerAppConfiguration configuration = this.createConfiguration();
        return new ConfiguredProducerApp<>(producerApp, configuration);
    }

    public ProducerAppConfiguration createConfiguration() {
        final ProducerTopicConfig topics = this.createTopicConfig();
        final Map<String, String> kafkaConfig = this.getKafkaConfig();
        return ProducerAppConfiguration.builder()
                .topics(topics)
                .kafkaConfig(kafkaConfig)
                .build();
    }

    public ProducerTopicConfig createTopicConfig() {
        return ProducerTopicConfig.builder()
                .outputTopic(this.getOutputTopic())
                .extraOutputTopics(this.getExtraOutputTopics())
                .build();
    }

    protected abstract ProducerApp createApp();

    private ProducerRunner createRunner() {
        final ConfiguredProducerApp<ProducerApp> app = this.createConfiguredApp();
        final KafkaEndpointConfig endpointConfig = this.getEndpointConfig();
        return app.createRunner(endpointConfig);
    }

    private ProducerCleanUpRunner createCleanUpRunner() {
        final ConfiguredProducerApp<ProducerApp> app = this.createConfiguredApp();
        final KafkaEndpointConfig endpointConfig = this.getEndpointConfig();
        return app.createCleanUpRunner(endpointConfig);
    }
}
