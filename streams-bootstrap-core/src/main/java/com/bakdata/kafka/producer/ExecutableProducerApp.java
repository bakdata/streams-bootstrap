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

package com.bakdata.kafka.producer;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * A {@link ProducerApp} with a corresponding {@link ProducerTopicConfig} and Kafka configuration
 * @param <T> type of {@link ProducerApp}
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class ExecutableProducerApp<T extends ProducerApp>
        implements ExecutableApp<ProducerRunner, ProducerCleanUpRunner, ProducerExecutionOptions> {
    private final @NonNull ProducerTopicConfig topics;
    private final @NonNull Map<String, Object> kafkaProperties;
    private final @NonNull T app;

    /**
     * Create {@link ProducerCleanUpRunner} in order to clean application
     * @return {@link ProducerCleanUpRunner}
     */
    @Override
    public ProducerCleanUpRunner createCleanUpRunner() {
        final AppConfiguration<ProducerTopicConfig> configuration = this.createConfiguration();
        final ProducerCleanUpConfiguration configurer = this.app.setupCleanUp(configuration);
        return ProducerCleanUpRunner.create(this.topics, this.kafkaProperties, configurer);
    }

    /**
     * Create {@link ProducerRunner} in order to run application
     * @return {@link ProducerRunner}
     */
    @Override
    public ProducerRunner createRunner() {
        return this.createRunner(ProducerExecutionOptions.builder().build());
    }

    @Override
    public ProducerRunner createRunner(final ProducerExecutionOptions options) {
        final ProducerBuilder producerBuilder = new ProducerBuilder(this.topics, this.kafkaProperties);
        final AppConfiguration<ProducerTopicConfig> configuration = this.createConfiguration();
        this.app.setup(configuration);
        return new ProducerRunner(this.app.buildRunnable(producerBuilder));
    }

    @Override
    public void close() {
        this.app.close();
    }

    private AppConfiguration<ProducerTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.kafkaProperties);
    }
}
