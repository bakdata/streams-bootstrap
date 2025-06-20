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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Provides topic configuration for a {@link StreamsApp}
 */
@Builder
@Value
@EqualsAndHashCode
public class StreamsTopicConfig {

    @Builder.Default
    @NonNull List<String> inputTopics = emptyList();
    /**
     * Input topics that are identified by a label
     */
    @Builder.Default
    @NonNull
    Map<String, List<String>> labeledInputTopics = emptyMap();
    Pattern inputPattern;
    /**
     * Input patterns that are identified by a label
     */
    @Builder.Default
    @NonNull
    Map<String, Pattern> labeledInputPatterns = emptyMap();
    String outputTopic;
    /**
     * Output topics that are identified by a label
     */
    @Builder.Default
    @NonNull
    Map<String, String> labeledOutputTopics = emptyMap();
    String errorTopic;

    /**
     * Get input topics for a specified label
     *
     * @param label label of input topics
     * @return topic names
     */
    public List<String> getInputTopics(final String label) {
        final List<String> topics = this.labeledInputTopics.get(label);
        if (topics == null) {
            throw new IllegalArgumentException(String.format("No input topics for label '%s' available", label));
        }
        return topics;
    }

    /**
     * Get input pattern for a specified label
     *
     * @param label label of input pattern
     * @return topic pattern
     */
    public Pattern getInputPattern(final String label) {
        final Pattern pattern = this.labeledInputPatterns.get(label);
        if (pattern == null) {
            throw new IllegalArgumentException(String.format("No input pattern for label '%s' available", label));
        }
        return pattern;
    }

    /**
     * Get output topic for a specified label
     *
     * @param label label of output topic
     * @return topic name
     */
    public String getOutputTopic(final String label) {
        final String topic = this.labeledOutputTopics.get(label);
        if (topic == null) {
            throw new IllegalArgumentException(String.format("No output topic for label '%s' available", label));
        }
        return topic;
    }
}
