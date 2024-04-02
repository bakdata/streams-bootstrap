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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public interface StreamsApp extends AutoCloseable {
    int DEFAULT_PRODUCTIVE_REPLICATION_FACTOR = 3;

    /**
     * Method to close resources outside of {@link KafkaStreams}. Will be called by default on  and on
     * transitioning to {@link State#ERROR}.
     */
    @Override
    default void close() {
        //do nothing by default
    }

    /**
     * Create a {@link StateListener} to use for Kafka Streams.
     *
     * @return {@code StateListener}.
     * @see KafkaStreams#setStateListener(StateListener)
     */
    default StateListener getStateListener() {
        return new NoOpStateListener();
    }

    /**
     * Create a {@link StreamsUncaughtExceptionHandler} to use for Kafka Streams.
     *
     * @return {@code StreamsUncaughtExceptionHandler}.
     * @see KafkaStreams#setUncaughtExceptionHandler(StreamsUncaughtExceptionHandler)
     */
    default StreamsUncaughtExceptionHandler getUncaughtExceptionHandler() {
        return new DefaultStreamsUncaughtExceptionHandler();
    }

    /**
     * Build the Kafka Streams topology to be run by the app
     *
     * @param builder builder to use for building the topology
     */
    void buildTopology(TopologyBuilder builder, boolean cleanUp);

    /**
     * This must be set to a unique value for every application interacting with your kafka cluster to ensure internal
     * state encapsulation. Could be set to: className-inputTopic-outputTopic
     */
    String getUniqueAppId(StreamsTopicConfig topics);

    /**
     * <p>This method should give a default configuration to run your streaming application with.</p>
     * If {@link KafkaApplication#schemaRegistryUrl} is set {@link SpecificAvroSerde} is set as the default key, value
     * serde. Otherwise, the {@link StringSerde} is configured as the default key, value serde. To add a custom
     * configuration please add a similar method to your custom application class:
     * <pre>{@code
     *   protected Properties createKafkaProperties() {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Properties kafkaConfig = super.createKafkaProperties();
     *       kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default Kafka Streams configuration
     */
    default Map<String, Object> createKafkaProperties(@NonNull final StreamsOptions options) {
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

    default void setupCleanUp(final StreamsCleanUpRunner cleanUpRunner) {
        // do nothing by default
    }

    default void onStreamsStart(final KafkaStreams streams) {
        // do nothing by default
    }
}
