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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ExecutableProducerAppTest {

    @Mock
    private Consumer<ProducerSetupConfiguration> setup;
    @Mock
    private Supplier<ProducerCleanUpConfiguration> setupCleanUp;

    @Test
    void shouldCallSetupWhenCreatingRunner() {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        final ProducerAppConfiguration configuration = ProducerAppConfiguration.builder()
                .topics(topics)
                .build();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                .brokers("localhost:9092")
                .build();
        final ExecutableProducerApp<ProducerApp> executableApp = configuredApp.withEndpoint(endpointConfig);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(endpointConfig);
        executableApp.createRunner();
        verify(this.setup).accept(ProducerSetupConfiguration.builder()
                .kafkaProperties(kafkaProperties)
                .topics(topics)
                .build());
    }

    @Test
    void shouldCallSetupWhenCreatingRunnerWithOptions() {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        final ProducerAppConfiguration configuration = ProducerAppConfiguration.builder()
                .topics(topics)
                .build();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                .brokers("localhost:9092")
                .build();
        final ExecutableProducerApp<ProducerApp> executableApp = configuredApp.withEndpoint(endpointConfig);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(endpointConfig);
        executableApp.createRunner(ProducerExecutionOptions.builder().build());
        verify(this.setup).accept(ProducerSetupConfiguration.builder()
                .kafkaProperties(kafkaProperties)
                .topics(topics)
                .build());
    }

    @Test
    void shouldCallSetupCleanUpWhenCreatingCleanUpRunner() {
        final ProducerTopicConfig topics = ProducerTopicConfig.builder()
                .outputTopic("output")
                .build();
        final ProducerAppConfiguration configuration = ProducerAppConfiguration.builder()
                .topics(topics)
                .build();
        final ConfiguredProducerApp<ProducerApp> configuredApp =
                new ConfiguredProducerApp<>(new TestProducer(), configuration);
        final KafkaEndpointConfig endpointConfig = KafkaEndpointConfig.builder()
                .brokers("localhost:9092")
                .build();
        final ExecutableProducerApp<ProducerApp> executableApp = configuredApp.withEndpoint(endpointConfig);
        when(this.setupCleanUp.get()).thenReturn(new ProducerCleanUpConfiguration());
        executableApp.createCleanUpRunner();
        verify(this.setupCleanUp).get();
    }

    private class TestProducer implements ProducerApp {

        @Override
        public void setup(final ProducerSetupConfiguration configuration) {
            ExecutableProducerAppTest.this.setup.accept(configuration);
        }

        @Override
        public ProducerCleanUpConfiguration setupCleanUp(final ProducerSetupConfiguration configuration) {
            return ExecutableProducerAppTest.this.setupCleanUp.get();
        }

        @Override
        public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
            return () -> {};
        }
    }
}
