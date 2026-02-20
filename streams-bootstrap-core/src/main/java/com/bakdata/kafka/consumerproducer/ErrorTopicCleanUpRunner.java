/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.CleanUpRunner;
import com.bakdata.kafka.admin.AdminClientX;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Delete the error topic specified by a {@link ConsumerProducerTopicConfig}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorTopicCleanUpRunner implements CleanUpRunner {
    private final String errorTopic;
    private final @NonNull Map<String, Object> kafkaProperties;

    /**
     * Create a new {@code ErrorTopicCleanUpRunner}
     *
     * @param errorTopic error topic to delete (may be {@code null})
     * @param kafkaProperties configuration to connect to Kafka admin tools
     * @return {@code ErrorTopicCleanUpRunner}
     */
    public static ErrorTopicCleanUpRunner create(final String errorTopic,
            @NonNull final Map<String, Object> kafkaProperties) {
        return new ErrorTopicCleanUpRunner(errorTopic, kafkaProperties);
    }

    @Override
    public void close() {
        // do nothing
    }

    /**
     * Delete the error topic
     */
    @Override
    public void clean() {
        if (this.errorTopic == null) {
            return;
        }
        try (final AdminClientX adminClient = this.createAdminClient()) {
            final Task task = new Task(adminClient);
            task.clean();
        }
    }

    private AdminClientX createAdminClient() {
        return AdminClientX.create(this.kafkaProperties);
    }

    @RequiredArgsConstructor
    private class Task {
        private final @NonNull AdminClientX adminClient;

        private void clean() {
            this.adminClient.topics()
                    .topic(ErrorTopicCleanUpRunner.this.errorTopic).deleteIfExists();
        }
    }
}
