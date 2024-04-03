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

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.KafkaEndpointConfig.KafkaEndpointConfigBuilder;
import java.util.Map;
import java.util.function.Function;
import lombok.experimental.UtilityClass;

/**
 * Utility class that provides helpers for using Fluent Kafka Streams Tests with {@link ConfiguredStreamsApp}
 */
@UtilityClass
public class StreamsBootstrapTopologyFactory {

    /**
     * Create a {@code TestTopology} from a {@code ConfiguredStreamsApp}. It injects are {@link KafkaEndpointConfig}
     * with configured Schema Registry.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopology} that uses topology and configuration provided by {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public static <K, V> TestTopology<K, V> createTopologyWithSchemaRegistry(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopology<>(app::createTopology, getKafkaPropertiesWithSchemaRegistryUrl(app));
    }

    /**
     * Create a {@code TestTopologyExtension} from a {@code ConfiguredStreamsApp}. It injects are
     * {@link KafkaEndpointConfig} with configured Schema Registry.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopologyExtension} that uses topology and configuration provided by {@code
     * ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public static <K, V> TestTopologyExtension<K, V> createTopologyExtensionWithSchemaRegistry(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopologyExtension<>(app::createTopology, getKafkaPropertiesWithSchemaRegistryUrl(app));
    }

    /**
     * Create a {@code TestTopology} from a {@code ConfiguredStreamsApp}. It injects are {@link KafkaEndpointConfig}
     * without configured Schema Registry.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopology} that uses topology and configuration provided by {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public static <K, V> TestTopology<K, V> createTopology(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopology<>(app::createTopology, getKafkaProperties(app));
    }

    /**
     * Create a {@code TestTopologyExtension} from a {@code ConfiguredStreamsApp}. It injects are
     * {@link KafkaEndpointConfig} without configured Schema Registry.
     *
     * @param app ConfiguredStreamsApp to create TestTopology from
     * @param <K> Default type of keys
     * @param <V> Default type of values
     * @return {@code TestTopologyExtension} that uses topology and configuration provided by
     * {@code ConfiguredStreamsApp}
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     * @see ConfiguredStreamsApp#createTopology(Map)
     */
    public static <K, V> TestTopologyExtension<K, V> createTopologyExtension(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return new TestTopologyExtension<>(app::createTopology, getKafkaProperties(app));
    }

    /**
     * Get Kafka properties from a {@code ConfiguredStreamsApp} after using a {@link KafkaEndpointConfig} with
     * configured Schema Registry.
     *
     * @param app ConfiguredStreamsApp to get Kafka properties of
     * @return Kafka properties
     * @see ConfiguredStreamsApp#getKafkaProperties(KafkaEndpointConfig)
     */
    public static Function<String, Map<String, Object>> getKafkaPropertiesWithSchemaRegistryUrl(
            final ConfiguredStreamsApp<? extends StreamsApp> app) {
        return schemaRegistryUrl -> {
            final KafkaEndpointConfig endpointConfig = newEndpointConfig()
                    .schemaRegistryUrl(schemaRegistryUrl)
                    .build();
            return app.getKafkaProperties(endpointConfig);
        };
    }

    private static Map<String, Object> getKafkaProperties(final ConfiguredStreamsApp<? extends StreamsApp> app) {
        final KafkaEndpointConfig endpointConfig = createEndpointConfig();
        return app.getKafkaProperties(endpointConfig);
    }

    private static KafkaEndpointConfig createEndpointConfig() {
        return newEndpointConfig()
                .build();
    }

    private static KafkaEndpointConfigBuilder newEndpointConfig() {
        return KafkaEndpointConfig.builder()
                .brokers("localhost:9092");
    }

}
