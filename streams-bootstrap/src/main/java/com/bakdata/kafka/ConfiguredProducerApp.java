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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

@RequiredArgsConstructor
public class ConfiguredProducerApp<T extends ProducerApp> {
    private final @NonNull T app;
    private final @NonNull ProducerAppConfiguration configuration;

    private static Map<String, Object> createKafkaProperties(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        if (endpointConfig.isSchemaRegistryConfigured()) {
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        } else {
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        }

        return kafkaConfig;
    }

    /**
     * <p>This method creates the configuration to run a {@link StreamsApp}.</p>
     * Configuration is created in the following order
     * <ul>
     *     <li>
     *         {@link ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG} and
     *         {@link ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG} are configured based on
     *         {@link KafkaEndpointConfig#isSchemaRegistryConfigured()}.
     *         If Schema Registry is configured, {@link SpecificAvroSerializer} is used, otherwise
     *         {@link StringSerializer} is used.
     *      </li>
     *      <li>
     *          Configs provided by {@link ProducerApp#createKafkaProperties()}
     *      </li>
     *      <li>
     *          Configs provided by {@link ProducerAppConfiguration#getKafkaConfig()}
     *      </li>
     *      <li>
     *          Environment variables {see @{link {@link EnvironmentKafkaConfigParser#parseVariables(Map)}}}
     *      </li>
     *      <li>
     *          Configs provided by {@link KafkaEndpointConfig#createKafkaProperties()}
     *      </li>
     * </ul>
     *
     * @return Returns Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaConfig = createKafkaProperties(endpointConfig);
        kafkaConfig.putAll(this.app.createKafkaProperties());
        kafkaConfig.putAll(this.configuration.getKafkaConfig());
        kafkaConfig.putAll(EnvironmentKafkaConfigParser.parseVariables(System.getenv()));
        kafkaConfig.putAll(endpointConfig.createKafkaProperties());
        return kafkaConfig;
    }

    public ProducerCleanUpRunner createCleanUpRunner(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(endpointConfig);
        final ProducerCleanUpRunner cleanUpRunner =
                new ProducerCleanUpRunner(this.configuration.getTopics(), kafkaProperties);

        this.app.setupCleanUp(cleanUpRunner);
        return cleanUpRunner;
    }

    public ProducerRunner createRunner(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(endpointConfig);
        final ProducerBuilder producerBuilder = ProducerBuilder.builder()
                .topics(this.configuration.getTopics())
                .kafkaProperties(kafkaProperties)
                .build();
        return new ProducerRunner(() -> this.app.run(producerBuilder));
    }
}
