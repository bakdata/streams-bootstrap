package com.bakdata.common_kafka_streams.test_applications;

import com.bakdata.common.kafka.streams.TestRecord;
import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

@NoArgsConstructor
public class MirrorKeyWithAvro extends KafkaStreamsApplication {
    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final KStream<TestRecord, String> input = builder.stream(this.getInputTopic());
        input.to(this.getOutputTopic());
    }

    @Override
    public String getUniqueAppId() {
        return this.getClass().getSimpleName() + "-" + this.getInputTopic() + "-" + this.getOutputTopic();
    }

    @Override
    public Properties getKafkaProperties() {
        final Properties kafkaConfig = super.getKafkaProperties();
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kafkaConfig;
    }
}