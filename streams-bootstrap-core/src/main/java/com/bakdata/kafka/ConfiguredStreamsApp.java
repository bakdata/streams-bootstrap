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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * A {@link StreamsApp} with a corresponding {@link StreamsTopicConfig}
 * @param <T> type of {@link StreamsApp}
 */
@RequiredArgsConstructor
public class ConfiguredStreamsApp<T extends StreamsApp> implements ConfiguredApp<ExecutableStreamsApp<T>> {
    @Getter
    private final @NonNull T app;
    private final @NonNull StreamsAppConfiguration configuration;

    private static Map<String, Object> createBaseConfig() {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        // exactly once and order
        kafkaConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // compression
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                CompressionType.GZIP.toString());

        return kafkaConfig;
    }

    /**
     * <p>This method creates the configuration to run a {@link StreamsApp}.</p>
     * Configuration is created in the following order
     * <ul>
     *     <li>
     *         Exactly-once, in-order, and compression are configured:
     * <pre>
     * processing.guarantee=exactly_once_v2
     * producer.max.in.flight.requests.per.connection=1
     * producer.acks=all
     * producer.compression.type=gzip
     * </pre>
     *     </li>
     *     <li>
     *         Configs provided by {@link StreamsApp#createKafkaProperties()}
     *     </li>
     *     <li>
     *         Configs provided via environment variables (see
     *         {@link EnvironmentKafkaConfigParser#parseVariables(Map)})
     *     </li>
     *     <li>
     *         Configs provided by {@link RuntimeConfiguration#createKafkaProperties()}
     *     </li>
     *     <li>
     *         {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG} and
     *         {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG} is configured using
     *         {@link StreamsApp#defaultSerializationConfig()}
     *     </li>
     *     <li>
     *         {@link StreamsConfig#APPLICATION_ID_CONFIG} is configured using
     *         {@link StreamsApp#getUniqueAppId(StreamsAppConfiguration)}
     *     </li>
     * </ul>
     *
     * @param runtimeConfiguration configuration to run app with
     * @return Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final RuntimeConfiguration runtimeConfiguration) {
        final KafkaPropertiesFactory propertiesFactory = this.createPropertiesFactory(runtimeConfiguration);
        return propertiesFactory.createKafkaProperties(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, this.getUniqueAppId()
        ));
    }

    /**
     * Get unique application identifier of {@code StreamsApp}
     * @return unique application identifier
     * @see StreamsApp#getUniqueAppId(StreamsAppConfiguration)
     */
    public String getUniqueAppId() {
        final String uniqueAppId = this.app.getUniqueAppId(this.configuration);
        if (this.configuration.getUniqueAppId().isPresent() && !uniqueAppId.equals(
                this.configuration.getUniqueAppId().get())) {
            throw new IllegalArgumentException("Provided application ID does not match StreamsApp#getUniqueAppId()");
        }
        return Objects.requireNonNull(uniqueAppId);
    }

    /**
     * Create an {@code ExecutableStreamsApp} using the provided {@link RuntimeConfiguration}
     * @return {@code ExecutableStreamsApp}
     */
    @Override
    public ExecutableStreamsApp<T> withRuntimeConfiguration(final RuntimeConfiguration runtimeConfiguration) {
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(runtimeConfiguration);
        final Topology topology = this.createTopology(kafkaProperties);
        return ExecutableStreamsApp.<T>builder()
                .topology(topology)
                .kafkaProperties(kafkaProperties)
                .app(this.app)
                .topics(this.getTopics())
                .build();
    }

    public StreamsTopicConfig getTopics() {
        return this.configuration.getTopics();
    }

    /**
     * Create the topology of the Kafka Streams app
     *
     * @param kafkaProperties configuration that should be used by clients to configure Kafka utilities
     * @return topology of the Kafka Streams app
     */
    public Topology createTopology(final Map<String, Object> kafkaProperties) {
        final StreamsBuilderX streamsBuilder = new StreamsBuilderX(this.getTopics(), kafkaProperties);
        this.app.buildTopology(streamsBuilder);
        return streamsBuilder.build();
    }

    @Override
    public void close() {
        this.app.close();
    }

    private KafkaPropertiesFactory createPropertiesFactory(final RuntimeConfiguration runtimeConfig) {
        final Map<String, Object> baseConfig = createBaseConfig();
        return KafkaPropertiesFactory.builder()
                .baseConfig(baseConfig)
                .app(this.app)
                .runtimeConfig(runtimeConfig)
                .build();
    }

}
