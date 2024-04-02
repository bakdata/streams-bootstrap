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
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

@RequiredArgsConstructor
public class ConfiguredStreamsApp {
    private final @NonNull StreamsApp app;
    private final @NonNull StreamsAppConfiguration configuration;

    private static Map<String, Object> createKafkaProperties(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaConfig = new HashMap<>();

        if (endpointConfig.isSchemaRegistryConfigured()) {
            kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
            kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        } else {
            kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
            kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        }

        return kafkaConfig;
    }

    /**
     * <p>This method creates the configuration to run a {@link StreamsApp}.</p>
     * Configuration is created in the following order
     * <ul>
     *     <li>
     *         {@link StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG} and
     *         {@link StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG} are configured based on
     *         {@link KafkaEndpointConfig#isSchemaRegistryConfigured()}.
     *         If Schema Registry is configured, {@link SpecificAvroSerde} is used, otherwise {@link StringSerde} is
     *         used.
     *      </li>
     *      <li>
     *          Configs provided by {@link StreamsApp#createKafkaProperties(StreamsOptions)}
     *      </li>
     *      <li>
     *          Configs provided by {@link StreamsAppConfiguration#getKafkaConfig()}
     *      </li>
     *      <li>
     *          Environment variables {see @{link {@link EnvironmentKafkaConfigParser#parseVariables(Map)}}}
     *      </li>
     *      <li>
     *          Configs provided by {@link KafkaEndpointConfig#createKafkaProperties()}
     *      </li>
     *      <li>
     *          {@link StreamsConfig#APPLICATION_ID_CONFIG} is configured using
     *          {@link StreamsApp#getUniqueAppId(StreamsTopicConfig)}
     *      </li>
     * </ul>
     *
     * @return Returns Kafka configuration
     */
    public Map<String, Object> getKafkaProperties(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaConfig = createKafkaProperties(endpointConfig);
        kafkaConfig.putAll(this.app.createKafkaProperties(this.configuration.getOptions()));
        kafkaConfig.putAll(this.configuration.getKafkaConfig());
        kafkaConfig.putAll(EnvironmentKafkaConfigParser.parseVariables(System.getenv()));
        kafkaConfig.putAll(endpointConfig.createKafkaProperties());
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, this.app.getUniqueAppId(this.configuration.getTopics()));
        return kafkaConfig;
    }

    public Topology createTopology(final Map<String, Object> kafkaProperties) {
        return this.createTopology(kafkaProperties, false);
    }

    public StreamsCleanUpRunner createCleanUpRunner(final KafkaEndpointConfig endpointConfig) {
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(endpointConfig);
        final Topology topology = this.createTopology(kafkaProperties, true);
        final StreamsCleanUpRunner cleanUpRunner = StreamsCleanUpRunner.create(topology, kafkaProperties);
        cleanUpRunner.registerFinishHook(this.app::close);

        this.app.setupCleanUp(cleanUpRunner);
        return cleanUpRunner;
    }

    public StreamsRunner createRunner(final KafkaEndpointConfig endpointConfig) {
        return this.createRunner(endpointConfig, StreamsExecutionOptions.builder().build());
    }

    public StreamsRunner createRunner(final KafkaEndpointConfig endpointConfig,
            final StreamsExecutionOptions executionOptions) {
        final Map<String, Object> kafkaProperties = this.getKafkaProperties(endpointConfig);
        final Topology topology = this.createTopology(kafkaProperties);
        return StreamsRunner.builder()
                .topology(topology)
                .config(new StreamsConfig(kafkaProperties))
                .executionOptions(executionOptions)
                .hooks(this.createHooks())
                .build();
    }

    /**
     * Create the topology of the Kafka Streams app
     *
     * @return topology of the Kafka Streams app
     * @param kafkaProperties configuration that should be used by clients to configure Kafka utilities
     * @param cleanUp whether topology is created in cleanUp context
     */
    private Topology createTopology(final Map<String, Object> kafkaProperties, final boolean cleanUp) {
        final TopologyBuilder topologyBuilder = TopologyBuilder.builder()
                .topics(this.configuration.getTopics())
                .kafkaProperties(kafkaProperties)
                .build();
        this.app.buildTopology(topologyBuilder, cleanUp);
        return topologyBuilder.build();
    }

    private StreamsHooks createHooks() {
        return StreamsHooks.builder()
                .stateListener(this.app.getStateListener())
                .uncaughtExceptionHandler(this.app.getUncaughtExceptionHandler())
                .onStart(this.app::onStreamsStart)
                .onShutdown(this.app::close)
                .build();
    }

}
