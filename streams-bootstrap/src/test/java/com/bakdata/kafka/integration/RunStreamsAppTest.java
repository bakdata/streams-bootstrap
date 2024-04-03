///*
// * MIT License
// *
// * Copyright (c) 2023 bakdata
// *
// * Permission is hereby granted, free of charge, to any person obtaining a copy
// * of this software and associated documentation files (the "Software"), to deal
// * in the Software without restriction, including without limitation the rights
// * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// * copies of the Software, and to permit persons to whom the Software is
// * furnished to do so, subject to the following conditions:
// *
// * The above copyright notice and this permission notice shall be included in all
// * copies or substantial portions of the Software.
// *
// * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// * SOFTWARE.
// */
//
//package com.bakdata.kafka.integration;
//
//import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
//import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
//import static net.mguenther.kafka.junit.Wait.delay;
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//import com.bakdata.kafka.CloseFlagApp;
//import com.bakdata.kafka.TopologyBuilder;
//import com.bakdata.kafka.test_applications.ExtraInputTopics;
//import com.bakdata.kafka.test_applications.Mirror;
//import com.google.common.collect.ImmutableMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import lombok.Getter;
//import lombok.RequiredArgsConstructor;
//import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
//import net.mguenther.kafka.junit.KeyValue;
//import net.mguenther.kafka.junit.ReadKeyValues;
//import net.mguenther.kafka.junit.SendKeyValuesTransactional;
//import net.mguenther.kafka.junit.TopicConfig;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.streams.KafkaStreams.State;
//import org.apache.kafka.streams.KafkaStreams.StateListener;
//import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
//import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
//import org.apache.kafka.streams.kstream.KStream;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//
//@ExtendWith(MockitoExtension.class)
//class RunStreamsAppTest {
//    private static final int TIMEOUT_SECONDS = 10;
//    private EmbeddedKafkaCluster kafkaCluster;
//    private KafkaStreamsApplication app = null;
//    @Mock
//    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
//    @Mock
//    private StateListener stateListener;
//
//    @BeforeEach
//    void setup() {
//        this.kafkaCluster = provisionWith(defaultClusterConfig());
//        this.kafkaCluster.start();
//    }
//
//    @AfterEach
//    void teardown() throws InterruptedException {
//        if (this.app != null) {
//            this.app.close();
//            this.app = null;
//        }
//
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        this.kafkaCluster.stop();
//    }
//
//    @Test
//    void shouldRunApp() throws InterruptedException {
//        final String input = "input";
//        final String output = "output";
//        this.kafkaCluster.createTopic(TopicConfig.withName(input).useDefaults());
//        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
//        this.setupApp(new Mirror());
//        this.app.setInputTopics(List.of(input));
//        this.app.setOutputTopic(output);
//        this.runApp();
//        final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
//                SendKeyValuesTransactional.inTransaction(input, List.of(new KeyValue<>("foo", "bar")))
//                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                        .build();
//        this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, String.class)
//                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
//                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
//                .build()))
//                .hasSize(1);
//    }
//
//    @Test
//    void shouldUseMultipleExtraInputTopics() throws InterruptedException {
//        final String input1 = "input1";
//        final String input2 = "input2";
//        final String output = "output";
//        this.kafkaCluster.createTopic(TopicConfig.withName(input1).useDefaults());
//        this.kafkaCluster.createTopic(TopicConfig.withName(input2).useDefaults());
//        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
//        this.setupApp(new ExtraInputTopics());
//        this.app.setExtraInputTopics(Map.of("role", List.of(input1, input2)));
//        this.app.setOutputTopic(output);
//        this.runApp();
//        this.kafkaCluster.send(SendKeyValuesTransactional.inTransaction(input1, List.of(new KeyValue<>("foo", "bar")))
//                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                .build());
//        this.kafkaCluster.send(SendKeyValuesTransactional.inTransaction(input2, List.of(new KeyValue<>("foo", "baz")))
//                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                .build());
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        assertThat(this.kafkaCluster.read(ReadKeyValues.from(output, String.class, String.class)
//                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
//                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
//                .build()))
//                .hasSize(2);
//    }
//
//    @Test
//    void shouldCallCloseResourcesOnMissingInputTopic() throws InterruptedException {
//        final String input = "input";
//        final String output = "output";
//        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
//        final CloseResourcesApplication closeResourcesApplication = new CloseResourcesApplication();
//        this.setupApp(closeResourcesApplication);
//        this.app.setInputTopics(List.of(input));
//        this.app.setOutputTopic(output);
//        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
//        this.runApp();
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        assertThat(closeResourcesApplication.getResourcesClosed()).isEqualTo(1);
//        verify(this.uncaughtExceptionHandler).handle(any());
//        verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
//    }
//
//    @Test
//    void shouldCallCloseResourcesOnMapError() throws InterruptedException {
//        final String input = "input";
//        final String output = "output";
//        this.kafkaCluster.createTopic(TopicConfig.withName(input).useDefaults());
//        this.kafkaCluster.createTopic(TopicConfig.withName(output).useDefaults());
//        final CloseResourcesApplication closeResourcesApplication = new CloseResourcesApplication();
//        this.setupApp(closeResourcesApplication);
//        this.app.setInputTopics(List.of(input));
//        this.app.setOutputTopic(output);
//        when(this.uncaughtExceptionHandler.handle(any())).thenReturn(StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
//        this.runApp();
//        final SendKeyValuesTransactional<String, String> kvSendKeyValuesTransactionalBuilder =
//                SendKeyValuesTransactional.inTransaction(input, List.of(new KeyValue<>("foo", "bar")))
//                        .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                        .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)
//                        .build();
//        this.kafkaCluster.send(kvSendKeyValuesTransactionalBuilder);
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        assertThat(closeResourcesApplication.getResourcesClosed()).isEqualTo(1);
//        verify(this.uncaughtExceptionHandler).handle(any());
//        verify(this.stateListener).onChange(State.ERROR, State.PENDING_ERROR);
//    }
//
//    @Test
//    void shouldLeaveGroup() throws InterruptedException {
//        final CloseFlagApp closeApplication = this.createCloseFlagApp();
//        this.runApp();
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        this.app.stop();
//        assertThat(closeApplication.getLeaveGroup()).isTrue();
//    }
//
//    @Test
//    void shouldNotLeaveGroup() throws InterruptedException {
//        final CloseFlagApp closeApplication = this.createCloseFlagApp();
//        this.app.setKafkaConfig(ImmutableMap.<String, String>builder()
//                .putAll(this.app.getKafkaConfig())
//                .put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "foo")
//                .build());
//        this.runApp();
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        this.app.stop();
//        assertThat(closeApplication.getLeaveGroup()).isFalse();
//    }
//
//    @Test
//    void shouldLeaveGroupWithVolatileGroupId() throws InterruptedException {
//        final CloseFlagApp closeApplication = this.createCloseFlagApp();
//        this.app.setKafkaConfig(ImmutableMap.<String, String>builder()
//                .putAll(this.app.getKafkaConfig())
//                .put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "foo")
//                .build());
//        this.app.setVolatileGroupInstanceId(true);
//        this.runApp();
//        delay(TIMEOUT_SECONDS, TimeUnit.SECONDS);
//        this.app.stop();
//        assertThat(closeApplication.getLeaveGroup()).isTrue();
//    }
//
//    private CloseFlagApp createCloseFlagApp() {
//        final CloseFlagApp closeApplication = new CloseFlagApp();
//        this.setupApp(closeApplication);
//        this.app.setInputTopics(List.of("input"));
//        this.app.setOutputTopic("output");
//        return closeApplication;
//    }
//
//    private void setupApp(final KafkaStreamsApplication application) {
//        this.app = application;
//        this.app.setBrokers(this.kafkaCluster.getBrokerList());
//        this.app.setKafkaConfig(Map.of(
//                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"
//        ));
//    }
//
//    private void runApp() {
//        // run in Thread because the application blocks indefinitely
//        new Thread(this.app).start();
//    }
//
//    @Getter
//    @RequiredArgsConstructor
//    private class CloseResourcesApplication extends KafkaStreamsApplication {
//        private int resourcesClosed = 0;
//
//        @Override
//        public void buildTopology(final TopologyBuilder builder) {
//            final KStream<String, String> input = builder.streamInput();
//            builder.toOutput(input.map((k, v) -> {throw new RuntimeException();}));
//        }
//
//        @Override
//        public String getUniqueAppId() {
//            return this.getClass().getSimpleName() + "-" + this.getOutputTopic();
//        }
//
//        @Override
//        protected void closeResources() {
//            this.resourcesClosed++;
//        }
//
//        @Override
//        protected StreamsUncaughtExceptionHandler getUncaughtExceptionHandler() {
//            return RunStreamsAppTest.this.uncaughtExceptionHandler;
//        }
//
//        @Override
//        protected StateListener getStateListener() {
//            return RunStreamsAppTest.this.stateListener;
//        }
//    }
//}
