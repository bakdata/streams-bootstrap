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

import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ImprovedStreamsConfigTest {

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldGetAppId() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092"
                )
        );
        this.softly.assertThat(new ImprovedStreamsConfig(config).getAppId())
                .isEqualTo("test-app");
    }

    @Test
    void shouldGetBootstrapServersFromList() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("broker1:9092", "broker2:9092", "broker3:9092")
                )
        );
        this.softly.assertThat(new ImprovedStreamsConfig(config).getBoostrapServers())
                .isEqualTo(List.of("broker1:9092", "broker2:9092", "broker3:9092"));
    }

    @Test
    void shouldGetBootstrapServersFromString() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092"
                )
        );
        this.softly.assertThat(new ImprovedStreamsConfig(config).getBoostrapServers())
                .isEqualTo(List.of("broker1:9092", "broker2:9092", "broker3:9092"));
    }

    @Test
    void shouldGetOriginalKafkaProperties() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(
                        StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092"
                )
        );
        this.softly.assertThat(new ImprovedStreamsConfig(config).getKafkaProperties())
                .hasSize(2)
                .anySatisfy((key, value) -> {
                    this.softly.assertThat(key).isEqualTo(StreamsConfig.APPLICATION_ID_CONFIG);
                    this.softly.assertThat(value).isEqualTo("test-app");
                })
                .anySatisfy((key, value) -> {
                    this.softly.assertThat(key).isEqualTo(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
                    this.softly.assertThat(value).isEqualTo("broker1:9092");
                });
    }

    @Test
    void shouldHaveHostInfoIfApplicationServiceIsConfigure() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092",
                        StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9090"));
        this.softly.assertThat(new ImprovedStreamsConfig(config).getApplicationServer())
                .hasValue(new HostInfo("localhost", 9090));
    }

    @Test
    void shouldReturnEmptyHostInfoIfApplicationServiceIsNotConfigure() {
        final StreamsConfig config = new StreamsConfig(
                Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "test-app",
                        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        this.softly.assertThat(new ImprovedStreamsConfig(config).getApplicationServer())
                .isEmpty();
    }

}
