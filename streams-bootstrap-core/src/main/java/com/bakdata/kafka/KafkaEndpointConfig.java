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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Configuration to connect to Kafka infrastructure, i.e., brokers and optionally schema registry.
 */
@Builder
public class KafkaEndpointConfig {
    private final @NonNull String brokers;
    @Builder.Default
    private final Map<String, Object> properties = new HashMap<>();

    /**
     * Create Kafka properties to connect to infrastructure.
     * The following properties are configured:
     * <ul>
     *     <li>{@code bootstrap.servers}</li>
     *     <li>{@code schema.registry.url}</li>
     * </ul>
     * @return properties used for connecting to Kafka
     */
    public Map<String, Object> createKafkaProperties() {
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.putAll(this.properties);
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        return Collections.unmodifiableMap(kafkaConfig);
    }

}
