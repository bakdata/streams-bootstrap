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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Produced} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Produced
 */
public final class ProducedX<K, V> extends ModifierChain<Produced<K, V>, Configurator, ProducedX<K, V>> {

    private ProducedX(final Function<Configurator, Produced<K, V>> initializer) {
        super(initializer);
    }

    /**
     * @see Produced#keySerde(Serde)
     */
    public static <K, V> ProducedX<K, V> keySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return new ProducedX<>(configurator -> Produced.keySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Produced#keySerde(Serde)
     */
    public static <K, V> ProducedX<K, V> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Produced#valueSerde(Serde)
     */
    public static <K, V> ProducedX<K, V> valueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return new ProducedX<>(configurator -> Produced.valueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Produced#valueSerde(Serde)
     */
    public static <K, V> ProducedX<K, V> valueSerde(final Serde<V> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Produced#with(Serde, Serde)
     */
    public static <K, V> ProducedX<K, V> with(final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return new ProducedX<>(configurator -> Produced.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Produced#with(Serde, Serde)
     */
    public static <K, V> ProducedX<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    /**
     * @see Produced#as(String)
     */
    public static <K, V> ProducedX<K, V> as(final String processorName) {
        return new ProducedX<>(configurator -> Produced.as(processorName));
    }

    /**
     * @see Produced#streamPartitioner(StreamPartitioner)
     */
    public static <K, V> ProducedX<K, V> streamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return new ProducedX<>(configurator -> Produced.streamPartitioner(partitioner));
    }

    /**
     * @see Produced#withKeySerde(Serde)
     */
    public ProducedX<K, V> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify((produced, configurator) -> produced.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Produced#withKeySerde(Serde)
     */
    public ProducedX<K, V> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Produced#withValueSerde(Serde)
     */
    public ProducedX<K, V> withValueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return this.modify(
                (produced, configurator) -> produced.withValueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Produced#withValueSerde(Serde)
     */
    public ProducedX<K, V> withValueSerde(final Serde<V> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Produced#withStreamPartitioner(StreamPartitioner)
     */
    public ProducedX<K, V> withStreamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return this.modify(produced -> produced.withStreamPartitioner(partitioner));
    }

    /**
     * @see Produced#withName(String)
     */
    public ProducedX<K, V> withName(final String name) {
        return this.modify(produced -> produced.withName(name));
    }

    @Override
    protected ProducedX<K, V> newInstance(final Function<Configurator, Produced<K, V>> initializer) {
        return new ProducedX<>(initializer);
    }
}
