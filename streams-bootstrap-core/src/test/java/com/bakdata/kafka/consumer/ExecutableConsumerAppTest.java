/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

package com.bakdata.kafka.consumer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.RuntimeConfiguration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ExecutableConsumerAppTest {

    @Mock
    private Consumer<AppConfiguration<ConsumerTopicConfig>> setup;
    @Mock
    private Supplier<ConsumerCleanUpConfiguration> setupCleanUp;

    @Test
    void shouldCallSetupWhenCreatingRunner() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), topics);
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableConsumerApp<ConsumerApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(configuration);
        executableApp.createRunner();
        verify(this.setup).accept(new AppConfiguration<>(topics, kafkaProperties));
    }

    @Test
    void shouldCallSetupWhenCreatingRunnerWithOptions() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), topics);
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableConsumerApp<ConsumerApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(configuration);
        executableApp.createRunner(ConsumerExecutionOptions.builder().build());
        verify(this.setup).accept(new AppConfiguration<>(topics, kafkaProperties));
    }

    @Test
    void shouldCallSetupCleanUpWhenCreatingCleanUpRunner() {
        final ConsumerTopicConfig topics = ConsumerTopicConfig.builder()
                .inputTopics(List.of("input"))
                .build();
        final ConfiguredConsumerApp<ConsumerApp> configuredApp =
                new ConfiguredConsumerApp<>(new TestConsumer(), topics);
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableConsumerApp<ConsumerApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        when(this.setupCleanUp.get()).thenReturn(new ConsumerCleanUpConfiguration());
        executableApp.createCleanUpRunner();
        verify(this.setupCleanUp).get();
    }

    private class TestConsumer implements ConsumerApp {

        @Override
        public void setup(final AppConfiguration<ConsumerTopicConfig> configuration) {
            ExecutableConsumerAppTest.this.setup.accept(configuration);
        }

        @Override
        public ConsumerCleanUpConfiguration setupCleanUp(
                final AppConfiguration<ConsumerTopicConfig> configuration) {
            return ExecutableConsumerAppTest.this.setupCleanUp.get();
        }

        @Override
        public String getUniqueAppId(final ConsumerTopicConfig topics) {
            return "app-id";
        }

        @Override
        public ConsumerRunnable buildRunnable(final ConsumerBuilder builder) {
            return () -> {};
        }

        @Override
        public DeserializerConfig defaultSerializationConfig() {
            return new DeserializerConfig(ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        }
    }
}
