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

import static java.util.Collections.emptyMap;

import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Provides topic configuration for a {@link ProducerApp}
 */
@Builder
@Value
@EqualsAndHashCode
public class ProducerTopicConfig {

    String outputTopic;
    /**
     * Extra output topics that are identified by a role
     */
    @Builder.Default
    @NonNull
    Map<String, String> extraOutputTopics = emptyMap();

    /**
     * Get extra output topic for a specified role
     *
     * @param role role of extra output topic
     * @return topic name
     */
    public String getOutputTopic(final String role) {
        final String topic = this.extraOutputTopics.get(role);
        if (topic == null) {
            throw new IllegalArgumentException(String.format("No output topic for role '%s' available", role));
        }
        return topic;
    }
}
