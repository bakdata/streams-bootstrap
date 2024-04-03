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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

public interface StreamsApp {
    int DEFAULT_PRODUCTIVE_REPLICATION_FACTOR = 3;

    /**
     * Build the Kafka Streams topology to be run by the app
     *
     * @param builder builder to use for building the topology
     */
    void buildTopology(TopologyBuilder builder);

    /**
     * This must be set to a unique value for every application interacting with your kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-inputTopic-outputTopic
     */
    String getUniqueAppId(StreamsTopicConfig topics);

    /**
     * <p>This method should give a default configuration to run your streaming application with.</p>
     * To add a custom configuration please add a similar method to your custom application class:
     * <pre>{@code
     *   protected Map<String, Object> createKafkaProperties(StreamsOptions options) {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Map<String, Object> kafkaConfig = StreamsApp.super.createKafkaProperties(options);
     *       kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default Kafka Streams configuration
     */
    default Map<String, Object> createKafkaProperties(final StreamsOptions options) {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        // exactly once and order
        kafkaConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        // resilience
        if (options.isProductive()) {
            kafkaConfig.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, DEFAULT_PRODUCTIVE_REPLICATION_FACTOR);
        }

        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // compression
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");

        return kafkaConfig;
    }

    default StreamsCleanUpConfigurer setupCleanUp() {
        return new StreamsCleanUpConfigurer();
    }
}
