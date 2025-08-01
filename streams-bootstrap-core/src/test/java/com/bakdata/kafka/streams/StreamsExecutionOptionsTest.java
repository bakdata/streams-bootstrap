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

package com.bakdata.kafka.streams;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

class StreamsExecutionOptionsTest {

    @Test
    void shouldLeaveGroup() {
        final StreamsExecutionOptions options = StreamsExecutionOptions.builder()
                .build();
        assertThat(options.shouldLeaveGroup(emptyMap())).isTrue();
    }

    @Test
    void shouldNotLeaveGroup() {
        final StreamsExecutionOptions options = StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(false)
                .build();
        assertThat(options.shouldLeaveGroup(Map.of(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "foo"
        ))).isFalse();
    }

    @Test
    void shouldLeaveGroupWithVolatileGroupId() {
        final StreamsExecutionOptions options = StreamsExecutionOptions.builder()
                .volatileGroupInstanceId(true)
                .build();
        assertThat(options.shouldLeaveGroup(Map.of(
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "foo"
        ))).isTrue();
    }
}
