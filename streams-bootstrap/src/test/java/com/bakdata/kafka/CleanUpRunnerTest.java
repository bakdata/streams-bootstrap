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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.test_applications.WordCount;
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
        final File file = CleanUpRunner.createTemporaryPropertiesFile(wordCount.getStreamsApplicationId(),
                wordCount.getKafkaProperties());

        assertThat(file).exists();

        final Properties properties = new Properties();
        try (final FileInputStream inStream = new FileInputStream(file)) {
            properties.load(inStream);
        }

        final Properties expected = CleanUpRunner.toStringBasedProperties(wordCount.getKafkaProperties());
        assertThat(properties).containsAllEntriesOf(expected);
    }
}
