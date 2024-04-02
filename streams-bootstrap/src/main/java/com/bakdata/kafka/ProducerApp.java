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

public interface ProducerApp {

    void run(ProducerBuilder builder);


    /**
     * <p>This method should give a default configuration to run your producer application with.</p>
     * To add a custom configuration, please add a similar method to your custom application class:
     * <pre>{@code
     *   public Map<String, Object> createKafkaProperties() {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Map<String, Object> kafkaConfig = ProducerApp.super.createKafkaProperties();
     *       kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
     *       kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default Kafka configuration
     */
    default Map<String, Object> createKafkaProperties() {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        // exactly once and order
        kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        // compression
        kafkaConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return kafkaConfig;
    }

    default void setupCleanUp(final ProducerCleanUpRunner cleanUpRunner) {
        // do nothing by default
    }
}
