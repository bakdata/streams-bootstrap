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

package com.bakdata.kafka.streams.kstream;

import com.bakdata.kafka.Configurator;
import com.bakdata.kafka.Preconfigured;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Grouped} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Grouped
 */
public final class GroupedX<K, V> extends ModifierChain<Grouped<K, V>, Configurator, GroupedX<K, V>> {

    private GroupedX(final Function<Configurator, Grouped<K, V>> initializer) {
        super(initializer);
    }

    /**
     * @see Grouped#keySerde(Serde)
     */
    public static <K, V> GroupedX<K, V> keySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return new GroupedX<>(configurator -> Grouped.keySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Grouped#keySerde(Serde)
     */
    public static <K, V> GroupedX<K, V> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Grouped#valueSerde(Serde)
     */
    public static <K, V> GroupedX<K, V> valueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return new GroupedX<>(configurator -> Grouped.valueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Grouped#valueSerde(Serde)
     */
    public static <K, V> GroupedX<K, V> valueSerde(final Serde<V> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Grouped#with(Serde, Serde)
     */
    public static <K, V> GroupedX<K, V> with(final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return new GroupedX<>(configurator -> Grouped.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Grouped#with(Serde, Serde)
     */
    public static <K, V> GroupedX<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    /**
     * @see Grouped#as(String)
     */
    public static <K, V> GroupedX<K, V> as(final String name) {
        return new GroupedX<>(configurator -> Grouped.as(name));
    }

    /**
     * @see Grouped#withKeySerde(Serde)
     */
    public GroupedX<K, V> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify((grouped, configurator) -> grouped.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Grouped#withKeySerde(Serde)
     */
    public GroupedX<K, V> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Grouped#withValueSerde(Serde)
     */
    public GroupedX<K, V> withValueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return this.modify(
                (grouped, configurator) -> grouped.withValueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Grouped#withValueSerde(Serde)
     */
    public GroupedX<K, V> withValueSerde(final Serde<V> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Grouped#withName(String)
     */
    public GroupedX<K, V> withName(final String processorName) {
        return this.modify(grouped -> grouped.withName(processorName));
    }

    @Override
    protected GroupedX<K, V> newInstance(final Function<Configurator, Grouped<K, V>> initializer) {
        return new GroupedX<>(initializer);
    }

}
