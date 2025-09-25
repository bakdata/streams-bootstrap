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

package com.bakdata.kafka.producer.apps;

import com.bakdata.kafka.TestRecord;
import com.bakdata.kafka.producer.ProducerBuilder;
import com.bakdata.kafka.producer.ProducerRunnable;
import com.bakdata.kafka.producer.SchemaRegistryProducerApp;
import com.bakdata.kafka.producer.SerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AvroValueProducer implements SchemaRegistryProducerApp {
    @Override
    public ProducerRunnable buildRunnable(final ProducerBuilder builder) {
        return () -> {
            try (final Producer<String, TestRecord> producer = builder.createProducer()) {
                producer.send(new ProducerRecord<>(builder.getTopics().getOutputTopic(), "key",
                        TestRecord.newBuilder().setContent("value").build()));
            }
        };
    }

    @Override
    public SerializerConfig defaultSerializationConfig() {
        return new SerializerConfig(StringSerializer.class, SpecificAvroSerializer.class);
    }
}
