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
import java.util.concurrent.ConcurrentLinkedDeque;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;


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
@Command(description = "Run a Kafka Producer application")
public abstract class KafkaProducerApplication extends KafkaApplication {
    @ToString.Exclude
    // ConcurrentLinkedDeque required because calling #close() causes asynchronous #run() calls to finish and thus
    // concurrently iterating on #runners and removing from #runners
    private ConcurrentLinkedDeque<ExecutableProducerApp<ProducerApp>> runningApps = new ConcurrentLinkedDeque<>();

    @Override
    public void run() {
        try (final ExecutableProducerApp<ProducerApp> app = this.createExecutableApp()) {
            this.runningApps.add(app);
            final ProducerRunner runner = app.createRunner();
            runner.run();
            this.runningApps.remove(app);
        }
    }

    @Command(description = "Delete all output topics associated with the Kafka Producer application.")
    @Override
    public void clean() {
        try (final ExecutableProducerApp<ProducerApp> app = this.createExecutableApp()) {
            final ProducerCleanUpRunner cleanUpRunner = app.createCleanUpRunner();
            cleanUpRunner.clean();
        }
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

    public abstract ProducerApp createApp();

    @Override
    public void close() {
        super.close();
        this.stop();
    }

    public void stop() {
        this.runningApps.forEach(ExecutableProducerApp::close);
    }

    private ExecutableProducerApp<ProducerApp> createExecutableApp() {
        final ConfiguredProducerApp<ProducerApp> app = this.createConfiguredApp();
        final KafkaEndpointConfig endpointConfig = this.getEndpointConfig();
        return app.withEndpoint(endpointConfig);
    }
}
