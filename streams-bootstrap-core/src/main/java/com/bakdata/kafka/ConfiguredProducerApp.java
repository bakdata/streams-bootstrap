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

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * A {@link ProducerApp} with a corresponding {@link AppConfiguration}
 * @param <T> type of {@link ProducerApp}
 */
@RequiredArgsConstructor
public class ConfiguredProducerApp<T extends ProducerApp> implements ConfiguredApp<ExecutableProducerApp<T>> {
    @Getter
    private final @NonNull T app;
    private final @NonNull AppConfiguration<ProducerTopicConfig> configuration;

    private static Map<String, Object> createBaseConfig() {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        // compression
        kafkaConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return kafkaConfig;
    }

    /**
     * <p>This method creates the configuration to run a {@link ProducerApp}.</p>
     * Configuration is created in the following order
     * <ul>
     *     <li>
     * <pre>
     * max.in.flight.requests.per.connection=1
     * acks=all
     * compression.type=gzip
     * </pre>
     *     </li>
     *     <li>
     *         Configs provided by {@link ProducerApp#createKafkaProperties()}
     *     </li>
     *     <li>
     *         Configs provided via environment variables (see
     *         {@link EnvironmentStreamsConfigParser#parseVariables(Map)})
     *     </li>
     *     <li>
     *         Configs provided by {@link AppConfiguration#getKafkaConfig()}
     *     </li>
     *     <li>
     *         Configs provided by {@link KafkaEndpointConfig#createKafkaProperties()}
     *     </li>
     *     <li>
     *         {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} and
     *         {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} is configured using
     *         {@link ProducerApp#defaultSerializerConfig()}
     *     </li>
     * </ul>
     *
     * @param endpointConfig endpoint to run app on
     * @return Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final KafkaEndpointConfig endpointConfig) {
        final KafkaPropertiesFactory propertiesFactory = this.createPropertiesFactory(endpointConfig);
        final SerializerConfig serializerConfig = this.app.defaultSerializerConfig();
        return propertiesFactory.createKafkaProperties(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerConfig.getKeySerializer(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerConfig.getValueSerializer()
        ));
    }

    /**
     * Create an {@code ExecutableProducerApp} using the provided {@code KafkaEndpointConfig}
     * @return {@code ExecutableProducerApp}
     */
    @Override
    public ExecutableProducerApp<T> withEndpoint(final KafkaEndpointConfig endpointConfig) {
        final ProducerTopicConfig topics = this.getTopics();
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(endpointConfig);
        return new ExecutableProducerApp<>(topics, kafkaProperties, this.app);
    }

    /**
     * Get topic configuration
     * @return topic configuration
     */
    public ProducerTopicConfig getTopics() {
        return this.configuration.getTopics();
    }

    @Override
    public void close() {
        this.app.close();
    }

    private KafkaPropertiesFactory createPropertiesFactory(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> baseConfig = createBaseConfig();
        return KafkaPropertiesFactory.builder()
                .baseConfig(baseConfig)
                .app(this.app)
                .configuration(this.configuration)
                .endpointConfig(endpointConfig)
                .build();
    }
}
