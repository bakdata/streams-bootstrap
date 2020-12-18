package com.bakdata.common_kafka_streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.test_applications.WordCount;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class CleanUpRunnerTest {

    @Test
    void createTemporaryPropertiesFile() throws IOException {
        final WordCount wordCount = new WordCount();
        wordCount.setInputTopics(List.of("input"));
        final File file = CleanUpRunner.createTemporaryPropertiesFile(wordCount.getUniqueAppId(),
                wordCount.getKafkaProperties());

        assertThat(file.exists()).isTrue();

        final Properties properties = new Properties();
        try (final FileInputStream inStream = new FileInputStream(file)) {
            properties.load(inStream);
        }

        final Properties expected = CleanUpRunner.toStringBasedProperties(wordCount.getKafkaProperties());
        assertThat(properties).containsAllEntriesOf(expected);
    }
}