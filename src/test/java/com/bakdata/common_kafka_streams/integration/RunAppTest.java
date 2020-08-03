/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

package com.bakdata.common_kafka_streams.integration;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.common_kafka_streams.KafkaStreamsApplication;
import com.bakdata.common_kafka_streams.test_applications.Mirror;
import com.bakdata.schemaregistrymock.junit5.SchemaRegistryMockExtension;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RunAppTest {
    private static final int TIMEOUT_SECONDS = 10;
    @RegisterExtension
    final SchemaRegistryMockExtension schemaRegistryMockExtension = new SchemaRegistryMockExtension();
    private EmbeddedKafkaCluster kafkaCluster;
    private KafkaStreamsApplication app = null;

    @BeforeEach
    void setup() {
        this.kafkaCluster = provisionWith(useDefaults());
        this.kafkaCluster.start();
    }

    @AfterEach
    void teardown() throws InterruptedException {
        if (this.app != null) {
            this.app.close();
            this.app = null;
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        this.kafkaCluster.stop();
    }

    @Test
    void shouldRunApp() throws InterruptedException {
        final String input = "input";
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.forTopic(input).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic(output).useDefaults());
        this.app = new Mirror();
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        this.app.setInputTopics(List.of(input));
        this.app.setOutputTopic(output);
        this.app.run();
        final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
                SendKeyValuesTransactional.inTransaction(input, List.of(new KeyValue<>("foo", "bar")))
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .build();
        this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, String.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .build()))
                .hasSize(1);
    }

    @Test
    void shouldCallCloseResourcesOnMissingInputTopic() throws InterruptedException {
        final String input = "input";
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.forTopic(output).useDefaults());
        final CloseResourcesApplication closeResourcesApplication = new CloseResourcesApplication();
        this.app = closeResourcesApplication;
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        this.app.setInputTopics(List.of(input));
        this.app.setOutputTopic(output);
        this.app.run();
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertThat(closeResourcesApplication.getCalled()).isEqualTo(1);
    }

    @Test
    void shouldCallCloseResourcesOnMapError() throws InterruptedException {
        final String input = "input";
        final String output = "output";
        this.kafkaCluster.createTopic(TopicConfig.forTopic(input).useDefaults());
        this.kafkaCluster.createTopic(TopicConfig.forTopic(output).useDefaults());
        final CloseResourcesApplication2 closeResourcesApplication = new CloseResourcesApplication2();
        this.app = closeResourcesApplication;
        this.app.setBrokers(this.kafkaCluster.getBrokerList());
        this.app.setSchemaRegistryUrl(this.schemaRegistryMockExtension.getUrl());
        this.app.setInputTopics(List.of(input));
        this.app.setOutputTopic(output);
        this.app.run();
        final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
                SendKeyValuesTransactional.inTransaction(input, List.of(new KeyValue<>("foo", "bar")))
                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
                        .build();
        this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
        Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        // in Kafka 2.5.0, UncaughtExceptionHandler is only called after close
        this.app.close();
        assertThat(closeResourcesApplication.getCalled()).isEqualTo(1);
    }

    private static class CloseResourcesApplication extends KafkaStreamsApplication {
        @Getter
        private int called = 0;

        @Override
        public void buildTopology(final StreamsBuilder builder) {
            final KStream<String, String> input = builder.stream(this.getInputTopic());
            input.map((k, v) -> {throw new RuntimeException();}).to(this.getOutputTopic());
        }

        @Override
        public String getUniqueAppId() {
            return this.getClass().getSimpleName() + "-" + this.getInputTopic() + "-" + this.getOutputTopic();
        }

        @Override
        protected void closeResources() {
            this.called++;
        }

        @Override
        protected Properties createKafkaProperties() {
            final Properties kafkaProperties = super.createKafkaProperties();
            kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            return kafkaProperties;
        }
    }

    private static class CloseResourcesApplication2 extends KafkaStreamsApplication {
        @Getter
        private int called = 0;

        @Override
        public void buildTopology(final StreamsBuilder builder) {
            final KStream<String, String> input = builder.stream(this.getInputTopic());
            input.map((k, v) -> {throw new RuntimeException();}).to(this.getOutputTopic());
        }

        @Override
        public String getUniqueAppId() {
            return this.getClass().getSimpleName() + "-" + this.getInputTopic() + "-" + this.getOutputTopic();
        }

        @Override
        public void close() {
            if (this.getStreams() == null) {
                return;
            }
            this.getStreams().close();
        }

        @Override
        protected void closeResources() {
            this.called++;
        }

        @Override
        protected Properties createKafkaProperties() {
            final Properties kafkaProperties = super.createKafkaProperties();
            kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
            return kafkaProperties;
        }
    }
}
