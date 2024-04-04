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

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * A {@link StreamsApp} with a corresponding {@link Topology} and {@link StreamsConfig}
 * @param <T> type of {@link ProducerApp}
 */
@RequiredArgsConstructor
@Getter
public class ExecutableStreamsApp<T extends StreamsApp> implements AutoCloseable {

    private final @NonNull Topology topology;
    private final @NonNull StreamsConfig streamsConfig;
    private final @NonNull T app;

    /**
     * Create {@code StreamsCleanUpRunner} in order to clean application
     * @return {@code StreamsCleanUpRunner}
     */
    public StreamsCleanUpRunner createCleanUpRunner() {
        final StreamsCleanUpConfiguration configurer = this.app.setupCleanUp();
        return StreamsCleanUpRunner.create(this.topology, this.streamsConfig, configurer);
    }

    /**
     * Create {@code StreamsRunner} in order to run application with default {@link StreamsExecutionOptions}
     * @return {@code StreamsRunner}
     * @see StreamsRunner#StreamsRunner(Topology, StreamsConfig)
     */
    public StreamsRunner createRunner() {
        return new StreamsRunner(this.topology, this.streamsConfig);
    }

    /**
     * Create {@code StreamsRunner} in order to run application
     * @param executionOptions options for running Kafka Streams application
     * @return {@code StreamsRunner}
     * @see StreamsRunner#StreamsRunner(Topology, StreamsConfig, StreamsExecutionOptions)
     */
    public StreamsRunner createRunner(final StreamsExecutionOptions executionOptions) {
        return new StreamsRunner(this.topology, this.streamsConfig, executionOptions);
    }

    @Override
    public void close() {
        this.app.close();
    }
}
