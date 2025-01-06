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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;

@With
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfiguredConsumed<K, V> {

    private final Preconfigured<Serde<K>> keySerde;
    private final Preconfigured<Serde<V>> valueSerde;
    private final TimestampExtractor timestampExtractor;
    private final AutoOffsetReset offsetResetPolicy;
    private final String name;

    public static <K, V> ConfiguredConsumed<K, V> keySerde(final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, null);
    }

    public static <K, V> ConfiguredConsumed<K, V> valueSerde(final Preconfigured<Serde<V>> valueSerde) {
        return with(null, valueSerde);
    }

    public static <K, V> ConfiguredConsumed<K, V> with(final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new ConfiguredConsumed<>(keySerde, valueSerde, null, null, null);
    }

    public static <K, V> ConfiguredConsumed<K, V> as(final String processorName) {
        return new ConfiguredConsumed<>(null, null, null, null, processorName);
    }

    Consumed<K, V> configure(final Configurator configurator) {
        return Consumed.<K, V>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withOffsetResetPolicy(this.offsetResetPolicy)
                .withTimestampExtractor(this.timestampExtractor);
    }
}
