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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Consumed} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Consumed
 */
@With
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredConsumed<K, V> {

    private final @NonNull Preconfigured<Serde<K>> keySerde;
    private final @NonNull Preconfigured<Serde<V>> valueSerde;
    private final TimestampExtractor timestampExtractor;
    private final AutoOffsetReset offsetResetPolicy;
    private final String name;

    /**
     * Create an instance of {@code ConfiguredConsumed} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code ConfiguredConsumed}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredConsumed<K, V> keySerde(final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code ConfiguredConsumed} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConfiguredConsumed}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredConsumed<K, V> valueSerde(final Preconfigured<Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * @see Consumed#with(Serde, Serde)
     */
    public static <K, V> ConfiguredConsumed<K, V> with(final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new ConfiguredConsumed<>(keySerde, valueSerde, null, null, null);
    }

    /**
     * @see Consumed#as(String)
     */
    public static <K, V> ConfiguredConsumed<K, V> as(final String processorName) {
        return new ConfiguredConsumed<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), null, null,
                processorName);
    }

    Consumed<K, V> configure(final Configurator configurator) {
        return Consumed.<K, V>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withOffsetResetPolicy(this.offsetResetPolicy)
                .withTimestampExtractor(this.timestampExtractor);
    }

}
