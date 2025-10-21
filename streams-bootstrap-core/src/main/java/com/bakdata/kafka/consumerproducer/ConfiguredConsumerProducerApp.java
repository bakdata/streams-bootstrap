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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.ConfiguredApp;
import com.bakdata.kafka.EnvironmentKafkaConfigParser;
import com.bakdata.kafka.KafkaPropertiesFactory;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.consumer.ConfiguredConsumerApp;
import com.bakdata.kafka.producer.ConfiguredProducerApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

// TODO maybe create specific consumerproducerappconfiguration?
// TODO we want to have specific one and maybe even get rid of pattern - if not supported?

/**
 * A {@link ConsumerProducerApp} with a corresponding {@link StreamsAppConfiguration}
 *
 * @param <T> type of {@link ConsumerProducerApp}
 */
public record ConfiguredConsumerProducerApp<T extends ConsumerProducerApp>(@NonNull T app,
                                                                           @NonNull StreamsAppConfiguration configuration)
        implements ConfiguredApp<ExecutableConsumerProducerApp<T>> {

    /**
     * <p>This method creates the configuration to run a {@link ConsumerProducerApp}.</p>
     * Configuration is created in the following order
     * <ul>
     *     <li> Producer:
     * <pre>
     * max.in.flight.requests.per.connection=1
     * acks=all
     * compression.type=gzip
     * </pre>
     *     </li>
     *     <li> Consumer:
     * <pre>
     * auto.offset.reset=earliest
     * </pre>
     *     </li>
     *     <li>
     *         Configs provided by {@link ConsumerProducerApp#createKafkaProperties()}
     *     </li>
     *     <li>
     *         Configs provided via environment variables (see
     *         {@link EnvironmentKafkaConfigParser#parseVariables(Map)})
     *     </li>
     *     <li>
     *         Configs provided by {@link RuntimeConfiguration#createKafkaProperties()}
     *     </li>
     *     <li>
     *         {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} and
     *         {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} and
     *         {@link ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG} and
     *         {@link ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG} is configured using
     *         {@link ConsumerProducerApp#defaultSerializationConfig()}
     *     </li>
     * </ul>
     *
     * @param runtimeConfiguration configuration to run app with
     * @return Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final RuntimeConfiguration runtimeConfiguration) {
        final Map<String, Object> config = ConfiguredProducerApp.createBaseConfig();
        config.putAll(ConfiguredConsumerApp.createBaseConfig());
        final KafkaPropertiesFactory propertiesFactory = this.createPropertiesFactory(runtimeConfiguration, config);
        return propertiesFactory.createKafkaProperties(Map.of(
                CommonClientConfigs.GROUP_ID_CONFIG, this.getUniqueAppId()
        ));
    }

    /**
     * Get unique application identifier of {@link ConsumerProducerApp}
     *
     * @return unique application identifier
     * @throws IllegalArgumentException if unique application identifier of {@link ConsumerProducerApp} is different
     * from provided application identifier in {@link StreamsAppConfiguration}
     * @see ConsumerProducerApp#getUniqueAppId(StreamsAppConfiguration)
     */
    public String getUniqueAppId() {
        final String uniqueAppId =
                Objects.requireNonNull(this.app.getUniqueAppId(this.configuration), "Application ID cannot be null");
        if (this.configuration.getUniqueAppId().map(configuredId -> !uniqueAppId.equals(configuredId)).orElse(false)) {
            throw new IllegalArgumentException(
                    "Provided application ID does not match ConsumerProducerApp#getUniqueAppId()");
        }
        return uniqueAppId;
    }

    /**
     * Create an {@code ExecutableConsumerProducerApp} using the provided {@link RuntimeConfiguration}
     *
     * @return {@code ExecutableConsumerProducerApp}
     */
    @Override
    public ExecutableConsumerProducerApp<T> withRuntimeConfiguration(final RuntimeConfiguration runtimeConfiguration) {
        final Map<String, Object> properties = this.getKafkaProperties(runtimeConfiguration);
        return ExecutableConsumerProducerApp.<T>builder()
                .consumerProperties(properties)
                .producerProperties(properties)
                .app(this.app)
                .topics(this.getTopics())
                .groupId(this.getUniqueAppId())
                .build();
    }

    /**
     * Get topic configuration
     *
     * @return topic configuration
     */
    public StreamsTopicConfig getTopics() {
        return this.configuration.getTopics();
    }

    @Override
    public void close() {
        this.app.close();
    }

    private KafkaPropertiesFactory createPropertiesFactory(final RuntimeConfiguration runtimeConfiguration,
            final Map<String, Object> baseConfig) {
        return KafkaPropertiesFactory.builder()
                .baseConfig(baseConfig)
                .app(this.app)
                .runtimeConfig(runtimeConfiguration)
                .build();
    }

}
