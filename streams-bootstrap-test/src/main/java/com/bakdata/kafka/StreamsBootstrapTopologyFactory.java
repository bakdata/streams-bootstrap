/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.experimental.UtilityClass;

@UtilityClass
public class StreamsBootstrapTopologyFactory {

    public static <K, V> TestTopology<K, V> createTopologyWithSchemaRegistry(final KafkaStreamsApplication app) {
        return new TestTopology<>(p -> {
            app.setSchemaRegistryUrl(p.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
            return app.createTopology();
        }, app.getKafkaProperties());
    }

    public static <K, V> TestTopologyExtension<K, V> createTopologyExtensionWithSchemaRegistry(
            final KafkaStreamsApplication app) {
        return new TestTopologyExtension<>(p -> {
            app.setSchemaRegistryUrl(p.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
            return app.createTopology();
        }, app.getKafkaProperties());
    }

    public static <K, V> TestTopology<K, V> createTopology(final KafkaStreamsApplication app) {
        return new TestTopology<>(app::createTopology, app.getKafkaProperties());
    }

    public static <K, V> TestTopologyExtension<K, V> createTopologyExtension(final KafkaStreamsApplication app) {
        return new TestTopologyExtension<>(app::createTopology, app.getKafkaProperties());
    }

}
