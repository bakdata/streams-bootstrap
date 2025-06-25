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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.tools.StreamsResetter;

/**
 * Resets the processing state of your Kafka Streams app
 */
@Slf4j
public final class StreamsResetterWrapper {

    private static final int EXIT_CODE_SUCCESS = 0;

    private StreamsResetterWrapper() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Run the <a href="https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html">Kafka
     * Streams Reset Tool</a>
     *
     * @param inputTopics list of input topics of the streams app
     * @param intermediateTopics list of intermediate topics of the streams app
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @param streamsAppConfig configuration properties of the streams app
     */
    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final Collection<String> allTopics, final ImprovedStreamsConfig streamsAppConfig) {
        runResetter(inputTopics, intermediateTopics, allTopics, streamsAppConfig.getAppId(),
                streamsAppConfig.getKafkaProperties(), streamsAppConfig.getBoostrapServers());
    }

    /**
     * Run the <a href="https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html">Kafka
     * Streams Reset Tool</a>
     *
     * @param inputTopics list of input topics of the streams app
     * @param intermediateTopics list of intermediate topics of the streams app
     * @param allTopics list of all topics that exists in the Kafka cluster
     * @param appId application ID of the streams app
     * @param kafkaProperties Kafka properties of the streams app
     * @param bootstrapServers list of bootstrap servers of the streams app
     */
    public static void runResetter(final Collection<String> inputTopics, final Collection<String> intermediateTopics,
            final Collection<String> allTopics, final String appId, final Map<String, Object> kafkaProperties,
            final List<String> bootstrapServers) {
        // StreamsResetter's internal AdminClient can only be configured with a properties file
        final File tempFile = createTemporaryPropertiesFile(appId, kafkaProperties);
        final Collection<String> argList = new ArrayList<>(List.of(
                "--application-id", appId,
                "--bootstrap-server", String.join(",", bootstrapServers),
                "--config-file", tempFile.toString()
        ));
        final Collection<String> existingInputTopics = filterExistingTopics(inputTopics, allTopics);
        if (!existingInputTopics.isEmpty()) {
            argList.addAll(List.of("--input-topics", String.join(",", existingInputTopics)));
        }
        final Collection<String> existingIntermediateTopics = filterExistingTopics(intermediateTopics, allTopics);
        if (!existingIntermediateTopics.isEmpty()) {
            argList.addAll(List.of("--intermediate-topics", String.join(",", existingIntermediateTopics)));
        }
        final String[] args = argList.toArray(String[]::new);
        final StreamsResetter resetter = new StreamsResetter();
        final int returnCode = resetter.execute(args);
        try {
            Files.delete(tempFile.toPath());
        } catch (final IOException e) {
            log.warn("Error deleting temporary property file", e);
        }
        if (returnCode != EXIT_CODE_SUCCESS) {
            throw new CleanUpException("Error running streams resetter. Exit code " + returnCode);
        }
    }

    static File createTemporaryPropertiesFile(final String appId, final Map<String, Object> config) {
        // Writing properties requires Map<String, String>
        final Properties parsedProperties = toStringBasedProperties(config);
        try {
            final File tempFile = File.createTempFile(appId + "-reset", "temp");
            try (final FileOutputStream out = new FileOutputStream(tempFile)) {
                parsedProperties.store(out, "");
            }
            return tempFile;
        } catch (final IOException e) {
            throw new CleanUpException("Could not run StreamsResetter", e);
        }
    }

    static Properties toStringBasedProperties(final Map<String, Object> config) {
        final Properties parsedProperties = new Properties();
        config.forEach((key, value) -> parsedProperties.setProperty(key, value.toString()));
        return parsedProperties;
    }

    private static Collection<String> filterExistingTopics(final Collection<String> topics,
            final Collection<String> allTopics) {
        return topics.stream()
                .filter(topicName -> {
                    final boolean exists = allTopics.contains(topicName);
                    if (!exists) {
                        log.warn("Not resetting missing topic {}", topicName);
                    }
                    return exists;
                })
                .collect(Collectors.toList());
    }
}
