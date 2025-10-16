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

package com.bakdata.kafka.consumerproducer;

import static java.util.Collections.emptyMap;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.ConfiguredApp;
import com.bakdata.kafka.EnvironmentKafkaConfigParser;
import com.bakdata.kafka.KafkaPropertiesFactory;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.consumer.ConfiguredConsumerApp;
import com.bakdata.kafka.producer.ConfiguredProducerApp;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import com.bakdata.kafka.streams.StreamsTopicConfig;
import java.util.Map;
import java.util.Objects;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

/**
 * A {@link ConsumerProducerApp} with a corresponding {@link AppConfiguration}
 *
 * @param <T> type of {@link ConsumerProducerApp}
 */
public record ConfiguredConsumerProducerApp<T extends ConsumerProducerApp>(@NonNull T app,
                                                                           @NonNull StreamsAppConfiguration configuration)
        implements
        ConfiguredApp<ExecutableConsumerProducerApp<T>> {
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
     *         {@link StreamsApp#getUniqueAppId(StreamsTopicConfig)}
     *     </li>
     * </ul>
     *
     * @param runtimeConfiguration configuration to run app with
     * @return Kafka configuration
     */
    public Map<String, Object> getKafkaConsumerProperties(final RuntimeConfiguration runtimeConfiguration) {
        final KafkaPropertiesFactory
                propertiesFactory =
                this.createPropertiesFactory(runtimeConfiguration, ConfiguredConsumerApp.createBaseConfig());
        return propertiesFactory.createKafkaProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, this.getUniqueAppId()
        ));
    }

    public Map<String, Object> getKafkaProducerProperties(final RuntimeConfiguration runtimeConfiguration) {
        final KafkaPropertiesFactory propertiesFactory =
                this.createPropertiesFactory(runtimeConfiguration, ConfiguredProducerApp.createBaseConfig());
        return propertiesFactory.createKafkaProperties(emptyMap());
    }

    /**
     * Get unique application identifier of {@link StreamsApp}
     *
     * @return unique application identifier
     * @throws IllegalArgumentException if unique application identifier of {@link StreamsApp} is different from
     * provided application identifier in {@link StreamsAppConfiguration}
     * @see StreamsApp#getUniqueAppId(StreamsAppConfiguration)
     */
    public String getUniqueAppId() {
        final String uniqueAppId =
                Objects.requireNonNull(this.app.getUniqueAppId(this.configuration), "Application ID cannot be null");
        if (this.configuration.getUniqueAppId().map(configuredId -> !uniqueAppId.equals(configuredId)).orElse(false)) {
            throw new IllegalArgumentException("Provided application ID does not match StreamsApp#getUniqueAppId()");
        }
        return uniqueAppId;
    }

    /**
     * Create an {@code ExecutableStreamsApp} using the provided {@link RuntimeConfiguration}
     *
     * @return {@code ExecutableStreamsApp}
     */
    @Override
    public ExecutableConsumerProducerApp<T> withRuntimeConfiguration(final RuntimeConfiguration runtimeConfiguration) {
        final Map<String, Object> consumerProperties = this.getKafkaConsumerProperties(runtimeConfiguration);
        final Map<String, Object> producerProperties = this.getKafkaProducerProperties(runtimeConfiguration);
        return ExecutableConsumerProducerApp.<T>builder()
                .consumerProperties(consumerProperties)
                .producerProperties(producerProperties)
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
