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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;

@Builder
class KafkaPropertiesFactory {
    private final @NonNull Map<String, Object> baseConfig;
    private final @NonNull App<?, ?> app;
    private final @NonNull AppConfiguration<?> configuration;
    private final @NonNull KafkaEndpointConfig endpointConfig;

    Map<String, Object> createKafkaProperties(final Map<String, Object> configOverrides) {
        return new Task().createKafkaProperties(configOverrides);
    }

    private class Task {
        private final Map<String, Object> kafkaConfig = new HashMap<>();

        private Map<String, Object> createKafkaProperties(final Map<String, Object> configOverrides) {
            this.putAll(KafkaPropertiesFactory.this.app.createKafkaProperties());
            final SerializationConfig serializationConfig =
                    KafkaPropertiesFactory.this.app.defaultSerializationConfig();
            this.putAll(EnvironmentStreamsConfigParser.parseVariables(System.getenv()));
            this.putAll(KafkaPropertiesFactory.this.configuration.getKafkaConfig());
            this.putAll(KafkaPropertiesFactory.this.endpointConfig.createKafkaProperties());
            this.putAll(serializationConfig.createProperties());
            this.putAllValidating(configOverrides);
            return Collections.unmodifiableMap(this.kafkaConfig);
        }

        private void putAllValidating(final Map<String, ?> configs) {
            this.validateNotSet(configs);
            this.putAll(configs);
        }

        private void putAll(final Map<String, ?> configs) {
            this.kafkaConfig.putAll(configs);
        }

        private void validateNotSet(final Map<String, ?> configs) {
            configs.keySet().forEach(this::validateNotSet);
        }

        private void validateNotSet(final String key) {
            if (this.kafkaConfig.containsKey(key)) {
                throw new IllegalArgumentException(String.format("'%s' has been configured already", key));
            }
        }
    }
}
