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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Produced} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Produced
 */
@With
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredProduced<K, V> {

    private final @NonNull Preconfigured<Serde<K>> keySerde;
    private final @NonNull Preconfigured<Serde<V>> valueSerde;
    private final StreamPartitioner<? super K, ? super V> streamPartitioner;
    private final String name;

    /**
     * @see Produced#keySerde(Serde) 
     */
    public static <K, V> ConfiguredProduced<K, V> keySerde(final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * @see Produced#valueSerde(Serde)
     */
    public static <K, V> ConfiguredProduced<K, V> valueSerde(final Preconfigured<Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * @see Produced#with(Serde, Serde) 
     */
    public static <K, V> ConfiguredProduced<K, V> with(final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new ConfiguredProduced<>(keySerde, valueSerde, null, null);
    }

    /**
     * @see Produced#as(String) 
     */
    public static <K, V> ConfiguredProduced<K, V> as(final String processorName) {
        return new ConfiguredProduced<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), null,
                processorName);
    }

    Produced<K, V> configure(final Configurator configurator) {
        return Produced.<K, V>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withStreamPartitioner(this.streamPartitioner);
    }

}
