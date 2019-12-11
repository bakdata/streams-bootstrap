package com.bakdata.common_kafka_streams.test_applications;

import com.bakdata.common.kafka.streams.TestRecord;
import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class ComplexTopologyApplication extends KafkaStreamsApplication {

    public static final String THROUGH_TOPIC = "through-topic";

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        final KStream<String, TestRecord> input = builder.stream(this.getInputTopic());

        final KTable<Windowed<String>, TestRecord> reduce = input.through(THROUGH_TOPIC)
                .groupByKey()
                .windowedBy(TimeWindows.of(5))
                .reduce((a, b) -> a);

        reduce.toStream()
                .map((k, v) -> KeyValue.pair(v.getContent(), v))
                .groupByKey()
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to(this.getOutputTopic(), Produced.with(Serdes.String(), Serdes.Long()));
    }

    @Override
    public String getUniqueAppId() {
        return this.getClass().getSimpleName() + "-" + this.getInputTopic() + "-" + this.getOutputTopic();
    }

    @Override
    public Properties getKafkaProperties() {
        final Properties kafkaConfig = super.getKafkaProperties();
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        return kafkaConfig;
    }
}
