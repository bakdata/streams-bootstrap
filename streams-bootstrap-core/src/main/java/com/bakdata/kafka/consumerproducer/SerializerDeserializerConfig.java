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

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.DeserializerConfig;
import com.bakdata.kafka.SerializationConfig;
import com.bakdata.kafka.producer.SerializerConfig;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Defines how to (de)serialize the data in a Kafka consumer or producer
 */
@RequiredArgsConstructor
@With
public class SerializerDeserializerConfig implements SerializationConfig {

    private final @NonNull SerializerConfig serializerConfig;
    private final @NonNull DeserializerConfig deserializerConfig;

    @Override
    public Map<String, Object> createProperties() {
        return Stream.concat(Stream.of(this.serializerConfig.createProperties()),
                        Stream.of(this.deserializerConfig.createProperties()))
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v2
                ));
    }

    public SerializerDeserializerConfig(final @NonNull Class<? extends Serializer> keySerializer,
            final @NonNull Class<? extends Serializer> valueSerializer,
            final @NonNull Class<? extends Deserializer> keyDeserializer,
            final @NonNull Class<? extends Deserializer> valueDeserializer) {
        this.serializerConfig = new SerializerConfig(keySerializer, valueSerializer);
        this.deserializerConfig = new DeserializerConfig(keyDeserializer, valueDeserializer);
    }
}
