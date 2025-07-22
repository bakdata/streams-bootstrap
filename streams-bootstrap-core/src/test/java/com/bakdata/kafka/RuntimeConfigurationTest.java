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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

class RuntimeConfigurationTest {

    @Test
    void shouldConfigureSessionTimeout() {
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final Map<String, Object> properties = Map.of(
                StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2
        );
        assertThat(configuration.with(properties).createKafkaProperties())
                .containsEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
    }

    @Test
    void shouldThrowIfBoostrapServersAlreadyConfigured() {
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final Map<String, String> properties = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        );
        assertThatThrownBy(() -> configuration.with(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property 'bootstrap.servers' already configured");
    }

}
