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

package com.bakdata.kafka;

import com.bakdata.kafka.HasTopicHooks.TopicHook;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Map;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class that provides helpers for removing schemas associated with topics
 */
@UtilityClass
@Slf4j
public class SchemaRegistryAppUtils {

    /**
     * Create a hook that cleans up schemas associated with a topic. It is expected that all necessary properties to
     * create a {@link SchemaRegistryClient} are part of {@code kafkaProperties}.
     *
     * @param kafkaProperties Kafka properties to create hook from
     * @return hook that cleans up schemas associated with a topic
     * @see HasTopicHooks#registerTopicHook(TopicHook)
     */
    public static TopicHook createTopicHook(final Map<String, Object> kafkaProperties) {
        final SchemaRegistryClient schemaRegistryClient =
                SchemaRegistryTopicHook.createSchemaRegistryClient(kafkaProperties);
        return new SchemaRegistryTopicHook(schemaRegistryClient);
    }

    /**
     * Create a hook that cleans up schemas associated with a topic. It is expected that all necessary properties to
     * create a {@link SchemaRegistryClient} are part of {@link AppConfiguration#getKafkaProperties()}.
     *
     * @param configuration Configuration to create hook from
     * @return hook that cleans up schemas associated with a topic
     * @see #createTopicHook(Map)
     */
    public static TopicHook createTopicHook(final AppConfiguration<?> configuration) {
        return createTopicHook(configuration.getKafkaProperties());
    }

    /**
     * Register a hook that cleans up schemas associated with a topic
     *
     * @param cleanUpConfiguration Configuration to register hook on
     * @param configuration Configuration to create hook from
     * @param <T> type of configuration
     * @return Configuration with registered topic hook
     * @see SchemaRegistryAppUtils#createTopicHook(AppConfiguration)
     */
    public static <T> T registerTopicHook(
            final HasTopicHooks<T> cleanUpConfiguration, final AppConfiguration<?> configuration) {
        return cleanUpConfiguration.registerTopicHook(createTopicHook(configuration));
    }

}
