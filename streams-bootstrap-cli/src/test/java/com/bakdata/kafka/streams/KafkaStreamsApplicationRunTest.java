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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.kafka.KafkaTest;
import com.bakdata.kafka.KafkaTestClient;
import com.bakdata.kafka.SenderBuilder.SimpleProducerRecord;
import com.bakdata.kafka.TestApplicationRunner;
import com.bakdata.kafka.streams.apps.Mirror;
import java.nio.file.Path;
import java.util.List;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaStreamsApplicationRunTest extends KafkaTest {
    @TempDir
    private Path stateDir;

    @Test
    void shouldRunApp() {
        final String input = "input";
        final String output = "output";
        try (final KafkaStreamsApplication<?> app = new SimpleKafkaStreamsApplication<>(Mirror::new)) {
            app.setInputTopics(List.of(input));
            app.setOutputTopic(output);
            final TestApplicationRunner runner = TestApplicationRunner.create(this.getBootstrapServers())
                    .withStateDir(this.stateDir)
                    .withNoStateStoreCaching()
                    .withSessionTimeout(SESSION_TIMEOUT);
            final KafkaTestClient testClient = runner.newTestClient();
            testClient.createTopic(output);
            runner.run(app);
            testClient.send()
                    .withKeySerializer(new StringSerializer())
                    .withValueSerializer(new StringSerializer())
                    .to(input, List.of(new SimpleProducerRecord<>("foo", "bar")));
            assertThat(testClient.read()
                    .withKeyDeserializer(new StringDeserializer())
                    .withValueDeserializer(new StringDeserializer())
                    .from(output, POLL_TIMEOUT))
                    .hasSize(1);
        }
    }
}
