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
import org.apache.kafka.streams.AutoOffsetReset;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Consumed} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Consumed
 */
public final class ConsumedX<K, V> extends ModifierChain<Consumed<K, V>, Configurator, ConsumedX<K, V>> {

    private ConsumedX(final Function<Configurator, Consumed<K, V>> initializer) {
        super(initializer);
    }

    /**
     * Create an instance of {@code ConsumedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code ConsumedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConsumedX<K, V> keySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code ConsumedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code ConsumedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConsumedX<K, V> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * Create an instance of {@code ConsumedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConsumedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConsumedX<K, V> valueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * Create an instance of {@code ConsumedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConsumedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConsumedX<K, V> valueSerde(final Serde<V> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Consumed#with(Serde, Serde)
     */
    public static <K, V> ConsumedX<K, V> with(final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return new ConsumedX<>(configurator -> Consumed.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Consumed#with(Serde, Serde)
     */
    public static <K, V> ConsumedX<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    /**
     * @see Consumed#with(TimestampExtractor)
     */
    public static <K, V> ConsumedX<K, V> with(final TimestampExtractor timestampExtractor) {
        return new ConsumedX<>(configurator -> Consumed.with(timestampExtractor));
    }

    /**
     * @see Consumed#with(Topology.AutoOffsetReset)
     */
    @Deprecated(since = "5.0.0")
    public static <K, V> ConsumedX<K, V> with(final Topology.AutoOffsetReset resetPolicy) {
        return new ConsumedX<>(configurator -> Consumed.with(resetPolicy));
    }

    /**
     * @see Consumed#with(AutoOffsetReset)
     */
    public static <K, V> ConsumedX<K, V> with(final AutoOffsetReset resetPolicy) {
        return new ConsumedX<>(configurator -> Consumed.with(resetPolicy));
    }

    /**
     * @see Consumed#as(String)
     */
    public static <K, V> ConsumedX<K, V> as(final String processorName) {
        return new ConsumedX<>(configurator -> Consumed.as(processorName));
    }

    /**
     * @see Consumed#withKeySerde(Serde)
     */
    public ConsumedX<K, V> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify((consumed, configurator) -> consumed.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Consumed#withKeySerde(Serde)
     */
    public ConsumedX<K, V> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Consumed#withValueSerde(Serde)
     */
    public ConsumedX<K, V> withValueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return this.modify(
                (consumed, configurator) -> consumed.withValueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Consumed#withValueSerde(Serde)
     */
    public ConsumedX<K, V> withValueSerde(final Serde<V> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Consumed#withOffsetResetPolicy(AutoOffsetReset)
     */
    public ConsumedX<K, V> withOffsetResetPolicy(final AutoOffsetReset offsetResetPolicy) {
        return this.modify(consumed -> consumed.withOffsetResetPolicy(offsetResetPolicy));
    }

    /**
     * @see Consumed#withOffsetResetPolicy(Topology.AutoOffsetReset)
     */
    @Deprecated(since = "5.0.0")
    public ConsumedX<K, V> withOffsetResetPolicy(final Topology.AutoOffsetReset offsetResetPolicy) {
        return this.modify(consumed -> consumed.withOffsetResetPolicy(offsetResetPolicy));
    }

    /**
     * @see Consumed#withTimestampExtractor(TimestampExtractor)
     */
    public ConsumedX<K, V> withTimestampExtractor(final TimestampExtractor timestampExtractor) {
        return this.modify(consumed -> consumed.withTimestampExtractor(timestampExtractor));
    }

    /**
     * @see Consumed#withName(String)
     */
    public ConsumedX<K, V> withName(final String processorName) {
        return this.modify(consumed -> consumed.withName(processorName));
    }

    @Override
    protected ConsumedX<K, V> newInstance(final Function<Configurator, Consumed<K, V>> initializer) {
        return new ConsumedX<>(initializer);
    }

}
