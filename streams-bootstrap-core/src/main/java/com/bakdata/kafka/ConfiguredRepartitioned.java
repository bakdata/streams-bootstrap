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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Repartitioned} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @see Repartitioned
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredRepartitioned<K, V> {

    @With
    private final @NonNull Preconfigured<Serde<K>> keySerde;
    @With
    private final @NonNull Preconfigured<Serde<V>> valueSerde;
    @With
    private final StreamPartitioner<K, V> streamPartitioner;
    @With
    private final String name;
    private final Integer numberOfPartitions;

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> keySerde(final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> valueSerde(final Preconfigured<Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided key and value serde
     * @param keySerde Serde to use for keys
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> with(final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new ConfiguredRepartitioned<>(keySerde, valueSerde, null, null, null);
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided name
     * @param name the name used as a processor name and part of the repartition topic name
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> as(final String name) {
        return new ConfiguredRepartitioned<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), null, name,
                null);
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided number of partitions for repartition topic
     * @param numberOfPartitions number of partitions
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> numberOfPartitions(final int numberOfPartitions) {
        return new ConfiguredRepartitioned<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), null, null,
                numberOfPartitions);
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided partitioner for repartition topic
     * @param partitioner partitioner to use
     * @return a new instance of {@code ConfiguredRepartitioned}
     * @param <K> type of keys
     * @param <V> type of values
     */
    public static <K, V> ConfiguredRepartitioned<K, V> streamPartitioner(final StreamPartitioner<K, V> partitioner) {
        return new ConfiguredRepartitioned<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), partitioner,
                null, null);
    }

    /**
     * Create an instance of {@code ConfiguredRepartitioned} with provided number of partitions for repartition topic
     * @param numberOfPartitions number of partitions
     * @return a new instance of {@code ConfiguredRepartitioned}
     */
    public ConfiguredRepartitioned<K, V> withNumberOfPartitions(final int numberOfPartitions) {
        return new ConfiguredRepartitioned<>(this.keySerde, this.valueSerde, this.streamPartitioner, this.name,
                numberOfPartitions);
    }

    Repartitioned<K, V> configure(final Configurator configurator) {
        final Repartitioned<K, V> repartitioned = Repartitioned.<K, V>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withStreamPartitioner(this.streamPartitioner);
        return this.numberOfPartitions == null ? repartitioned : repartitioned
                .withNumberOfPartitions(this.numberOfPartitions);
    }

}
