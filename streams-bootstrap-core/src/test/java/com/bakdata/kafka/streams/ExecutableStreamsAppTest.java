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

package com.bakdata.kafka.streams;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.AppConfiguration;
import com.bakdata.kafka.RuntimeConfiguration;
import com.bakdata.kafka.streams.kstream.StreamsBuilderX;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ExecutableStreamsAppTest {

    @Mock
    private Consumer<AppConfiguration<StreamsTopicConfig>> setup;
    @Mock
    private Supplier<StreamsCleanUpConfiguration> setupCleanUp;

    @Test
    void shouldCallSetupWhenCreatingRunner() {
        final StreamsTopicConfig topics = StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), new StreamsAppConfiguration(topics));
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableStreamsApp<StreamsApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(configuration);
        executableApp.createRunner();
        verify(this.setup).accept(new AppConfiguration<>(topics, kafkaProperties));
    }

    @Test
    void shouldCallSetupWhenCreatingRunnerWithOptions() {
        final StreamsTopicConfig topics = StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), new StreamsAppConfiguration(topics));
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableStreamsApp<StreamsApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        final Map<String, Object> kafkaProperties = configuredApp.getKafkaProperties(configuration);
        executableApp.createRunner(StreamsExecutionOptions.builder().build());
        verify(this.setup).accept(new AppConfiguration<>(topics, kafkaProperties));
    }

    @Test
    void shouldCallSetupCleanUpWhenCreatingCleanUpRunner() {
        final StreamsTopicConfig topics = StreamsTopicConfig.builder()
                .inputTopics(List.of("input"))
                .outputTopic("output")
                .build();
        final ConfiguredStreamsApp<StreamsApp> configuredApp =
                new ConfiguredStreamsApp<>(new TestApplication(), new StreamsAppConfiguration(topics));
        final RuntimeConfiguration configuration = RuntimeConfiguration.create("localhost:9092");
        final ExecutableStreamsApp<StreamsApp> executableApp =
                configuredApp.withRuntimeConfiguration(configuration);
        when(this.setupCleanUp.get()).thenReturn(new StreamsCleanUpConfiguration());
        executableApp.createCleanUpRunner();
        verify(this.setupCleanUp).get();
    }

    private class TestApplication implements StreamsApp {

        @Override
        public void setup(final AppConfiguration<StreamsTopicConfig> configuration) {
            ExecutableStreamsAppTest.this.setup.accept(configuration);
        }

        @Override
        public StreamsCleanUpConfiguration setupCleanUp(
                final AppConfiguration<StreamsTopicConfig> setupConfiguration) {
            return ExecutableStreamsAppTest.this.setupCleanUp.get();
        }

        @Override
        public void buildTopology(final StreamsBuilderX builder) {
            builder.streamInput()
                    .toOutputTopic();
        }

        @Override
        public String getUniqueAppId(final StreamsAppConfiguration configuration) {
            return "foo";
        }

        @Override
        public SerdeConfig defaultSerializationConfig() {
            return new SerdeConfig(StringSerde.class, StringSerde.class);
        }
    }
}
