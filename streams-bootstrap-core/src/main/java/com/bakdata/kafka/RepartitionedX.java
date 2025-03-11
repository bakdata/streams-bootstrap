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

import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Repartitioned} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Repartitioned
 */
public final class RepartitionedX<K, V> extends ModifierChain<Repartitioned<K, V>, Configurator, RepartitionedX<K, V>> {

    private RepartitionedX(final Function<Configurator, Repartitioned<K, V>> initializer) {
        super(initializer);
    }

    /**
     * Create an instance of {@code RepartitionedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code RepartitionedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> RepartitionedX<K, V> keySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code RepartitionedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code RepartitionedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> RepartitionedX<K, V> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * Create an instance of {@code RepartitionedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code RepartitionedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> RepartitionedX<K, V> valueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * Create an instance of {@code RepartitionedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code RepartitionedX}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> RepartitionedX<K, V> valueSerde(final Serde<V> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Repartitioned#with(Serde, Serde)
     */
    public static <K, V> RepartitionedX<K, V> with(final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return new RepartitionedX<>(configurator -> Repartitioned.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Repartitioned#with(Serde, Serde)
     */
    public static <K, V> RepartitionedX<K, V> with(final Serde<K> keySerde, final Serde<V> valueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    /**
     * @see Repartitioned#as(String)
     */
    public static <K, V> RepartitionedX<K, V> as(final String name) {
        return new RepartitionedX<>(configurator -> Repartitioned.as(name));
    }

    /**
     * @see Repartitioned#numberOfPartitions(int)
     */
    public static <K, V> RepartitionedX<K, V> numberOfPartitions(final int numberOfPartitions) {
        return new RepartitionedX<>(configurator -> Repartitioned.numberOfPartitions(numberOfPartitions));
    }

    /**
     * @see Repartitioned#streamPartitioner(StreamPartitioner)
     */
    public static <K, V> RepartitionedX<K, V> streamPartitioner(final StreamPartitioner<K, V> partitioner) {
        return new RepartitionedX<>(configurator -> Repartitioned.streamPartitioner(partitioner));
    }

    /**
     * @see Repartitioned#withNumberOfPartitions(int)
     */
    public RepartitionedX<K, V> withNumberOfPartitions(final int numberOfPartitions) {
        return this.modify(repartitioned -> repartitioned.withNumberOfPartitions(numberOfPartitions));
    }

    /**
     * @see Repartitioned#withKeySerde(Serde)
     */
    public RepartitionedX<K, V> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify(
                (repartitioned, configurator) -> repartitioned.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Repartitioned#withKeySerde(Serde)
     */
    public RepartitionedX<K, V> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Repartitioned#withValueSerde(Serde)
     */
    public RepartitionedX<K, V> withValueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return this.modify((repartitioned, configurator) -> repartitioned.withValueSerde(
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Repartitioned#withValueSerde(Serde)
     */
    public RepartitionedX<K, V> withValueSerde(final Serde<V> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Repartitioned#withStreamPartitioner(StreamPartitioner)
     */
    public RepartitionedX<K, V> withStreamPartitioner(final StreamPartitioner<K, V> partitioner) {
        return this.modify(repartitioned -> repartitioned.withStreamPartitioner(partitioner));
    }

    /**
     * @see Repartitioned#withName(String)
     */
    public RepartitionedX<K, V> withName(final String name) {
        return this.modify(repartitioned -> repartitioned.withName(name));
    }

    @Override
    protected RepartitionedX<K, V> newInstance(final Function<Configurator, Repartitioned<K, V>> initializer) {
        return new RepartitionedX<>(initializer);
    }
}
