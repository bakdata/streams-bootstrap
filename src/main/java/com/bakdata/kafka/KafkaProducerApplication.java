/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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
import com.bakdata.kafka.util.SchemaTopicClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.util.Properties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.jooq.lambda.Seq;
import picocli.CommandLine;


/**
 * <p>The base class of the entry point of a producer application.</p>
 * This class provides common configuration options, e.g., {@link #brokers}, for producer applications. Hereby it
 * automatically populates the passed in command line arguments with matching environment arguments {@link
 * EnvironmentArgumentsParser}. To implement your producer application inherit from this class and add your custom
 * options. Call {@link #startApplication(KafkaProducerApplication, String[])} with a fresh instance of your class from
 * your main.
 */
@ToString(callSuper = true)
@Getter
@Setter
@RequiredArgsConstructor
@Slf4j
public abstract class KafkaProducerApplication extends KafkaApplication {
    /**
     * This variable is usually set on application start. When the application is running in debug mode it is used to
     * reconfigure the child app package logger. On default it points to the package of this class allowing to execute
     * the run method independently.
     */
    private static String appPackageName = KafkaProducerApplication.class.getPackageName();

    /**
     * <p>This methods needs to be called in the executable custom application class inheriting from
     * {@code KafkaProducerApplication}.</p>
     *
     * @param app An instance of the custom application class.
     * @param args Arguments passed in by the custom application class.
     */
    protected static void startApplication(final KafkaProducerApplication app, final String[] args) {
        appPackageName = app.getClass().getPackageName();
        final String[] populatedArgs = addEnvironmentVariablesArguments(args);
        final int exitCode = new CommandLine(app).execute(populatedArgs);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        log.info("Starting application");
        if (this.debug) {
            Configurator.setLevel("com.bakdata", Level.DEBUG);
            Configurator.setLevel(appPackageName, Level.DEBUG);
        }
        log.debug(this.toString());
        if (this.cleanUp) {
            this.runCleanUp();
        } else {
            this.runApplication();
        }
    }

    protected abstract void runApplication();

    /**
     * <p>This method should give a default configuration to run your producer application with.</p>
     * To add a custom configuration please add a similar method to your custom application class:
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

        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        // exactly once and order
        kafkaConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // compression
        kafkaConfig.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        kafkaConfig.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokers);
        return kafkaConfig;
    }

    protected <K, V> KafkaProducer<K, V> createProducer() {
        final Properties properties = new Properties();
        properties.putAll(this.getKafkaProperties());
        return new KafkaProducer<>(properties);
    }

    /**
     * This methods deletes all output topics.
     */
    protected void runCleanUp() {
        try (final ImprovedAdminClient improvedAdminClient = this.createAdminClient()) {
            this.cleanUpRun(improvedAdminClient.getSchemaTopicClient());
        }
    }

    protected void cleanUpRun(final SchemaTopicClient schemaTopicClient) {
        final Iterable<String> outputTopics = this.getAllOutputTopics();

        outputTopics.forEach(schemaTopicClient::deleteTopicAndResetSchemaRegistry);
        try {
            Thread.sleep(RESET_SLEEP_MS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error waiting for clean up", e);
        }
    }

    private Iterable<String> getAllOutputTopics() {
        return Seq.of(this.getOutputTopic())
                .concat(this.extraOutputTopics.values());
    }
}
