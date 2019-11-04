/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

package com.bakdata.common_kafka_streams.util;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.net.SocketTimeoutException;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.AfterEach;

public abstract class ErrorCaptureTopologyTest {
    protected TestTopology<Integer, String> topology = null;

    // Make protected if you need to overwrite this method in your test class
    private static Properties getKafkaProperties() {
        final Properties kafkaConfig = new Properties();

        // exactly once and order
        kafkaConfig.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        kafkaConfig.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 1);

        kafkaConfig.setProperty(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        // topology
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy");
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, IntegerSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");

        return kafkaConfig;
    }

    @AfterEach
    void tearDown() {
        if (this.topology != null) {
            this.topology.stop();
        }
    }

    protected static RuntimeException createSchemaRegistryTimeoutException() {
        return new SerializationException(new SocketTimeoutException());
    }

    protected void createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        this.buildTopology(builder);
        final Topology topology = builder.build();
        this.topology = new TestTopology<>(topology, getKafkaProperties());
        this.topology.start();
    }

    protected abstract void buildTopology(StreamsBuilder builder);
}
