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

package com.bakdata.kafka.streams;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.producer.ProducerApp;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * A {@link StreamsApp} with a corresponding {@link Topology} and {@link StreamsConfig}
 *
 * @param <T> type of {@link ProducerApp}
 */
@Builder(access = AccessLevel.PACKAGE)
@Getter
public class ExecutableStreamsApp<T extends StreamsApp>
        implements ExecutableApp<StreamsRunner, StreamsCleanUpRunner, StreamsExecutionOptions> {

    @Getter
    private final @NonNull Topology topology;
    @Getter
    private final @NonNull Map<String, Object> kafkaProperties;
    @Getter
    private final @NonNull T app;
    @Getter
    private final @NonNull StreamsTopicConfig topics;

    /**
     * Create {@code StreamsCleanUpRunner} in order to clean application
     *
     * @return {@code StreamsCleanUpRunner}
     */
    @Override
    public StreamsCleanUpRunner createCleanUpRunner() {
        final StreamsCleanUpConfiguration configurer = this.app.setupCleanUp(this.createConfiguration());
        return StreamsCleanUpRunner.create(this.topology, this.getConfig(), configurer);
    }

    /**
     * Create {@code StreamsRunner} in order to run application with default {@link StreamsExecutionOptions}
     *
     * @return {@code StreamsRunner}
     * @see StreamsRunner#StreamsRunner(Topology, StreamsConfig)
     */
    @Override
    public StreamsRunner createRunner() {
        this.app.setup(this.createConfiguration());
        return new StreamsRunner(this.topology, this.getConfig());
    }

    /**
     * Create {@code StreamsRunner} in order to run application
     *
     * @param executionOptions options for running Kafka Streams application
     * @return {@code StreamsRunner}
     * @see StreamsRunner#StreamsRunner(Topology, StreamsConfig, StreamsExecutionOptions)
     */
    @Override
    public StreamsRunner createRunner(final StreamsExecutionOptions executionOptions) {
        this.app.setup(this.createConfiguration());
        return new StreamsRunner(this.topology, this.getConfig(), executionOptions);
    }

    @Override
    public void close() {
        this.app.close();
    }

    public StreamsConfig getConfig() {
        return new StreamsConfig(this.kafkaProperties);
    }

    private AppConfiguration<StreamsTopicConfig> createConfiguration() {
        return new AppConfiguration<>(this.topics, this.kafkaProperties);
    }
}
