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

import com.bakdata.kafka.util.ImprovedAdminClient;
import com.bakdata.kafka.util.SchemaClient;
import com.bakdata.kafka.util.TopicClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jooq.lambda.Seq;


/**
 * <p>The base class of the entry point of a producer application.</p>
 * This class provides common configuration options, e.g., {@link #brokers}, for producer applications. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments
 * {@link EnvironmentArgumentsParser}. To implement your producer application inherit from this class and add your
 * custom options. Call {@link #startApplication(KafkaApplication, String[])} with a fresh instance of your class from
 * your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
public abstract class KafkaProducerApplication extends KafkaApplication {

    private static void deleteTopicAndResetSchema(final TopicClient topicClient,
            final Optional<SchemaClient> schemaClient,
            final String outputTopic) {
        topicClient.deleteTopicIfExists(outputTopic);
        schemaClient.ifPresent(client -> client.resetSchemaRegistry(outputTopic));
    }

    @Override
    public void run() {
        super.run();

        if (this.cleanUp) {
            this.runCleanUp();
        } else {
            this.runApplication();
        }
    }

    protected abstract void runApplication();

    /**
     * <p>This method should give a default configuration to run your producer application with.</p>
     * If {@link KafkaApplication#schemaRegistryUrl} is set {@link SpecificAvroSerializer} is set as the default key,
     * value serializer. Otherwise, {@link StringSerializer} is configured as the default key, value serializer. To add
     * a custom configuration, please add a similar method to your custom application class:
     * <pre>{@code
     *   protected Properties createKafkaProperties() {
     *       # Try to always use the kafka properties from the super class as base Map
     *       Properties kafkaConfig = super.createKafkaProperties();
     *       kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
     *       kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GenericAvroSerializer.class);
     *       return kafkaConfig;
     *   }
     * }</pre>
     *
     * @return Returns a default Kafka configuration {@link Properties}
     */
    protected Properties createKafkaProperties() {
        final Properties kafkaConfig = new Properties();

        // exactly once and order
        kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // compression
        kafkaConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        this.configureDefaultSerializer(kafkaConfig);
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        return kafkaConfig;
    }

    protected <K, V> KafkaProducer<K, V> createProducer() {
        final Properties properties = new Properties();
        properties.putAll(this.getKafkaProperties());
        return new KafkaProducer<>(properties);
    }

    /**
     * This method deletes all output topics.
     */
    protected void runCleanUp() {
        try (final ImprovedAdminClient improvedAdminClient = this.createAdminClient()) {
            this.cleanUpRun(improvedAdminClient);
        }
    }

    protected void cleanUpRun(final ImprovedAdminClient improvedAdminClient) {
        final TopicClient topicClient = improvedAdminClient.getTopicClient();
        final Optional<SchemaClient> schemaClient = improvedAdminClient.getSchemaClient();

        final Iterable<String> outputTopics = this.getAllOutputTopics();
        outputTopics.forEach(topic -> deleteTopicAndResetSchema(topicClient, schemaClient, topic));

        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CleanUpException("Error waiting for clean up", e);
        }
    }

    private void configureDefaultSerializer(final Properties kafkaConfig) {
        if (this.schemaRegistryUrl == null) {
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        } else {
            kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
            kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
            kafkaConfig.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        }
    }

    private Iterable<String> getAllOutputTopics() {
        return Seq.of(this.outputTopic)
                .concat(this.extraOutputTopics.values());
    }
}
