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

import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.consumer.ConfiguredConsumerApp;
import com.bakdata.kafka.consumer.ConsumerApp;
import com.bakdata.kafka.consumer.ConsumerCleanUpRunner;
import com.bakdata.kafka.consumer.ConsumerRunner;
import com.bakdata.kafka.consumer.ExecutableConsumerApp;
import com.bakdata.kafka.streams.ConfiguredStreamsApp;
import com.bakdata.kafka.streams.ExecutableStreamsApp;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsCleanUpRunner;
import com.bakdata.kafka.streams.StreamsRunner;
import java.nio.file.Path;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;

@UtilityClass
public class TestHelper {
    public static <K, V> KeyValue<K, V> toKeyValue(final ConsumerRecord<K, V> consumerRecord) {
        return new KeyValue<>(consumerRecord.key(), consumerRecord.value());
    }

    public static ExecutableStreamsApp<StreamsApp> createExecutableApp(final ConfiguredStreamsApp<StreamsApp> app,
            final RuntimeConfiguration runtimeConfiguration, final Path stateDir) {
        return createExecutableApp(app, runtimeConfiguration.withStateDir(stateDir));
    }

    public static ExecutableStreamsApp<StreamsApp> createExecutableApp(
            final ConfiguredStreamsApp<StreamsApp> app, final RuntimeConfiguration runtimeConfiguration) {
        return app.withRuntimeConfiguration(runtimeConfiguration
                .withNoStateStoreCaching()
                .withSessionTimeout(KafkaTest.SESSION_TIMEOUT));
    }

    public static ExecutableConsumerApp<ConsumerApp> createExecutableApp(final ConfiguredConsumerApp<ConsumerApp> app,
            final RuntimeConfiguration runtimeConfiguration) {
        return app.withRuntimeConfiguration(runtimeConfiguration);
    }

    public static void run(final ExecutableStreamsApp<?> app) {
        try (final StreamsRunner runner = app.createRunner()) {
            runAsync(runner);
            // Wait until stream application has consumed all data
            KafkaTest.awaitProcessing(app);
        }
    }

    public static void run(final ExecutableConsumerApp<?> app) {
        try (final ConsumerRunner runner = app.createRunner()) {
            runAsync(runner);
            // Wait until stream application has consumed all data
            KafkaTest.awaitProcessing(app);
        }
    }

    public static void runWithoutAwait(final ExecutableConsumerApp<?> app) {
        try (final ConsumerRunner runner = app.createRunner()) {
            runAsync(runner);
        }
    }

    public static void run(final ExecutableApp<? extends Runner, ?, ?> executableApp) {
        executableApp.createRunner().run();
    }

    public static void reset(final ExecutableApp<?, StreamsCleanUpRunner, ?> app) {
        try (final StreamsCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    public static void resetConsumer(final ExecutableApp<?, ConsumerCleanUpRunner, ?> app) {
        try (final ConsumerCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

    public static void clean(final ExecutableApp<?, ? extends CleanUpRunner, ?> app) {
        try (final CleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.clean();
        }
    }
}
