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
import org.apache.kafka.streams.kstream.Grouped;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Grouped} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Grouped
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class AutoGrouped<K, V> {

    @With
    private final @NonNull Preconfigured<Serde<K>> keySerde;
    @With
    private final @NonNull Preconfigured<Serde<V>> valueSerde;
    @With
    private final String name;

    /**
     * @see Grouped#keySerde(Serde)
     */
    public static <K, V> AutoGrouped<K, V> keySerde(final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * @see Grouped#valueSerde(Serde)
     */
    public static <K, V> AutoGrouped<K, V> valueSerde(final Preconfigured<Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * @see Grouped#with(Serde, Serde)
     */
    public static <K, V> AutoGrouped<K, V> with(final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new AutoGrouped<>(keySerde, valueSerde, null);
    }

    /**
     * @see Grouped#as(String)
     */
    public static <K, V> AutoGrouped<K, V> as(final String name) {
        return new AutoGrouped<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), name);
    }

    Grouped<K, V> configure(final Configurator configurator) {
        return Grouped.<K, V>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde));
    }

}
