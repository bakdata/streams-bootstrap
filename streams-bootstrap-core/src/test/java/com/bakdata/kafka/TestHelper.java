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

import static java.util.Collections.emptyMap;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import lombok.Getter;
import lombok.experimental.UtilityClass;

@UtilityClass
public class TestHelper {
    public static Thread run(final StreamsRunner runner) {
        // run in Thread because the application blocks indefinitely
        final Thread thread = new Thread(runner);
        final UncaughtExceptionHandler handler = new CapturingUncaughtExceptionHandler();
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        return thread;
    }

    static <K, V> TestTopology<K, V> startApp(final ConfiguredStreamsApp<StreamsApp> app) {
        final TestTopology<K, V> topology =
                new TestTopologyFactory().createTopology(app);
        topology.start();
        return topology;
    }

    static ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app,
            final StreamsTopicConfig topicConfig) {
        return configureApp(app, topicConfig, emptyMap());
    }

    static ConfiguredStreamsApp<StreamsApp> configureApp(final StreamsApp app,
            final StreamsTopicConfig topicConfig,
            final Map<String, String> config) {
        return new ConfiguredStreamsApp<>(app, new AppConfiguration<>(topicConfig, config));
    }

    @Getter
    public static class CapturingUncaughtExceptionHandler implements UncaughtExceptionHandler {
        private Throwable lastException;

        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            this.lastException = e;
        }
    }
}
