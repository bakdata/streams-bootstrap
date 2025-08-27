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

package com.bakdata.kafka.consumer;

import com.bakdata.kafka.ConfiguredApp;
import com.bakdata.kafka.EnvironmentKafkaConfigParser;
import com.bakdata.kafka.KafkaPropertiesFactory;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.streams.StreamsApp;
import com.bakdata.kafka.streams.StreamsAppConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * A {@link ConsumerApp} with a corresponding {@link ConsumerTopicConfig}
 *
 * @param <T> type of {@link ConsumerApp}
 */
@RequiredArgsConstructor
@Getter
public class ConfiguredConsumerApp<T extends ConsumerApp> implements ConfiguredApp<ExecutableConsumerApp<T>> {
    private final @NonNull T app;
    private final @NonNull ConsumerAppConfiguration configuration;

    public static Map<String, Object> createBaseConfig() {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

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
     *         {@link EnvironmentKafkaConfigParser#parseVariables(Map)})
     *     </li>
     *     <li>
     *         Configs provided by {@link RuntimeConfiguration#createKafkaProperties()}
     *     </li>
     *     <li>
     *         {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} and
     *         {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} is configured using
     *         {@link ProducerApp#defaultSerializationConfig()}
     *     </li>
     * </ul>
     *
     * @param runtimeConfiguration configuration to run app with
     * @return Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final RuntimeConfiguration runtimeConfiguration) {
        final KafkaPropertiesFactory propertiesFactory = this.createPropertiesFactory(runtimeConfiguration);
        return propertiesFactory.createKafkaProperties(Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, this.getUniqueAppId()
        ));
    }

    /**
     * Get unique application identifier of {@link StreamsApp}
     * @return unique application identifier
     * @see StreamsApp#getUniqueAppId(StreamsAppConfiguration)
     * @throws IllegalArgumentException if unique application identifier of {@link StreamsApp} is different from
     * provided application identifier in {@link StreamsAppConfiguration}
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
     * Create an {@code ExecutableProducerApp} using the provided {@link RuntimeConfiguration}
     *
     * @return {@code ExecutableProducerApp}
     */
    @Override
    public ExecutableConsumerApp<T> withRuntimeConfiguration(final RuntimeConfiguration runtimeConfiguration) {
        final ConsumerTopicConfig topics = this.getTopics();
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(runtimeConfiguration);
        return new ExecutableConsumerApp<>(topics, kafkaProperties, this.getUniqueAppId(), this.app);
    }

    /**
     * Get topic configuration
     *
     * @return topic configuration
     */
    public ConsumerTopicConfig getTopics() {
        return this.configuration.getTopics();
    }

    @Override
    public void close() {
        this.app.close();
    }

    private KafkaPropertiesFactory createPropertiesFactory(final RuntimeConfiguration runtimeConfiguration) {
        final Map<String, Object> baseConfig = createBaseConfig();
        return KafkaPropertiesFactory.builder()
                .baseConfig(baseConfig)
                .app(this.app)
                .runtimeConfig(runtimeConfiguration)
                .build();
    }
}
