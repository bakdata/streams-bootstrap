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

import static com.bakdata.kafka.KafkaTest.awaitProcessing;
import static java.util.concurrent.CompletableFuture.runAsync;

import com.bakdata.kafka.ExecutableApp;
import com.bakdata.kafka.RuntimeConfiguration;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.assertj.core.api.SoftAssertions;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@UtilityClass
public class TestHelper {

    public static void assertContent(final SoftAssertions softly,
            final Collection<ConsumerRecord<String, String>> consumedRecords,
            final Iterable<? extends KeyValue<String, String>> expectedValues, final String description) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    final List<KeyValue<String, String>> consumedKeyValues = consumedRecords
                            .stream()
                            .map(com.bakdata.kafka.TestHelper::toKeyValue)
                            .toList();
                    softly.assertThat(consumedKeyValues)
                            .as(description)
                            .containsExactlyInAnyOrderElementsOf(expectedValues);
                });
    }

    public static ExecutableConsumerApp<ConsumerApp> createExecutableApp(final ConfiguredConsumerApp<ConsumerApp> app,
            final RuntimeConfiguration runtimeConfiguration) {
        return app.withRuntimeConfiguration(runtimeConfiguration);
    }

    public static void run(final ExecutableConsumerApp<?> app) {
        try (final ConsumerRunner runner = app.createRunner()) {
            runAsync(runner);
            // Wait until consumer application has consumed all data
            awaitProcessing(app);
        }
    }

    public static void reset(final ExecutableApp<?, ConsumerCleanUpRunner, ?> app) {
        try (final ConsumerCleanUpRunner cleanUpRunner = app.createCleanUpRunner()) {
            cleanUpRunner.reset();
        }
    }

}
