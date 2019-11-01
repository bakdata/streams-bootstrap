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
