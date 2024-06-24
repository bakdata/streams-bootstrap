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

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_SERVER_CONFIG;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Class for simplified access to configs provided by {@link StreamsConfig}
 */
@Value
public class ImprovedStreamsConfig {

    @NonNull StreamsConfig streamsConfig;

    /**
     * Get the application id of the underlying {@link StreamsConfig}
     * @return application id
     * @see StreamsConfig#APPLICATION_ID_CONFIG
     */
    public String getAppId() {
        return this.streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG);
    }

    /**
     * Get the bootstrap servers of the underlying {@link StreamsConfig}
     * @return comma separated bootstrap servers config
     * @see StreamsConfig#BOOTSTRAP_SERVERS_CONFIG
     */
    public String getBoostrapServers() {
        return String.join(",", this.streamsConfig.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    /**
     * Get all configs of the underlying {@link StreamsConfig}
     * @return Kafka configs
     * @see StreamsConfig#originals()
     */
    public Map<String, Object> getStreamsConfig() {
        return Collections.unmodifiableMap(this.streamsConfig.originals());
    }

    /**
     * Retrieves the host information based on the application server configuration.
     *
     * @return an {@code Optional} containing the {@link HostInfo} if the
     * {@link StreamsConfig#APPLICATION_SERVER_CONFIG} is set; otherwise, an empty {@code Optional}.
     */
    public Optional<HostInfo> getApplicationServer() {
        final String applicationServerConfig = this.streamsConfig.getString(APPLICATION_SERVER_CONFIG);
        return applicationServerConfig.isEmpty() ? Optional.empty()
                : Optional.of(createHostInfo(applicationServerConfig));
    }

    private static HostInfo createHostInfo(final String applicationServerConfig) {
        final String[] hostAndPort = applicationServerConfig.split(":");
        return new HostInfo(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }

}
