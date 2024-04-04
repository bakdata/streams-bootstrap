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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Provides topic configuration for a {@link StreamsApp}
 */
@Builder
@Value
public class StreamsTopicConfig {

    @Builder.Default
    @NonNull List<String> inputTopics = emptyList();
    /**
     * Extra input topics that are identified by a role
     */
    @Builder.Default
    @NonNull Map<String, List<String>> extraInputTopics = emptyMap();
    Pattern inputPattern;
    /**
     * Extra input patterns that are identified by a role
     */
    @Builder.Default
    @NonNull Map<String, Pattern> extraInputPatterns = emptyMap();
    String outputTopic;
    /**
     * Extra output topics that are identified by a role
     */
    @Builder.Default
    @NonNull Map<String, String> extraOutputTopics = emptyMap();
    String errorTopic;

    /**
     * Get extra input topics for a specified role
     *
     * @param role role of extra input topics
     * @return topic names
     */
    public List<String> getInputTopics(final String role) {
        final List<String> topics = this.extraInputTopics.get(role);
        Preconditions.checkNotNull(topics, "No input topics for role '%s' available", role);
        return topics;
    }

    /**
     * Get extra input pattern for a specified role
     *
     * @param role role of extra input pattern
     * @return topic pattern
     */
    public Pattern getInputPattern(final String role) {
        final Pattern pattern = this.extraInputPatterns.get(role);
        Preconditions.checkNotNull(pattern, "No input pattern for role '%s' available", role);
        return pattern;
    }

    /**
     * Get extra output topic for a specified role
     *
     * @param role role of extra output topic
     * @return topic name
     */
    public String getOutputTopic(final String role) {
        final String topic = this.extraOutputTopics.get(role);
        Preconditions.checkNotNull(topic, "No output topic for role '%s' available", role);
        return topic;
    }
}
