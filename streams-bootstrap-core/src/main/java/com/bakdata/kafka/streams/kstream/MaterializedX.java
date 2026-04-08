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
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Materialized} using {@link Configurator}
 * @param <K> type of keys
 * @param <V> type of values
 * @param <S> type of state store
 * @see Materialized
 */
public final class MaterializedX<K, V, S extends StateStore> extends
        ModifierChain<Materialized<K, V, S>, Configurator, MaterializedX<K, V, S>> {

    private MaterializedX(final Function<Configurator, Materialized<K, V, S>> initializer) {
        super(initializer);
    }

    /**
     * Create an instance of {@code MaterializedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code MaterializedX}
     * @param <K> type of keys
     * @param <V> type of values
     * @param <S> type of state store
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> keySerde(
            final Preconfigured<? extends Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code MaterializedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code MaterializedX}
     * @param <K> type of keys
     * @param <V> type of values
     * @param <S> type of state store
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * Create an instance of {@code MaterializedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code MaterializedX}
     * @param <K> type of keys
     * @param <V> type of values
     * @param <S> type of state store
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> valueSerde(
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * Create an instance of {@code MaterializedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code MaterializedX}
     * @param <K> type of keys
     * @param <V> type of values
     * @param <S> type of state store
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> valueSerde(final Serde<V> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Materialized#with(Serde, Serde)
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> with(
            final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return new MaterializedX<>(configurator -> Materialized.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Materialized#with(Serde, Serde)
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> with(
            final Serde<K> keySerde,
            final Serde<V> valueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    /**
     * @see Materialized#as(String)
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> as(final String storeName) {
        return new MaterializedX<>(configurator -> Materialized.as(storeName));
    }

    /**
     * @see Materialized#as(DslStoreSuppliers)
     */
    public static <K, V, S extends StateStore> MaterializedX<K, V, S> as(
            final DslStoreSuppliers storeSuppliers) {
        return new MaterializedX<>(configurator -> Materialized.as(storeSuppliers));
    }

    /**
     * @see Materialized#as(WindowBytesStoreSupplier)
     */
    public static <K, V> MaterializedX<K, V, WindowStore<Bytes, byte[]>> as(final WindowBytesStoreSupplier supplier) {
        return new MaterializedX<>(configurator -> Materialized.as(supplier));
    }

    /**
     * @see Materialized#as(SessionBytesStoreSupplier)
     */
    public static <K, V> MaterializedX<K, V, SessionStore<Bytes, byte[]>> as(final SessionBytesStoreSupplier supplier) {
        return new MaterializedX<>(configurator -> Materialized.as(supplier));
    }

    /**
     * @see Materialized#as(KeyValueBytesStoreSupplier)
     */
    public static <K, V> MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> as(
            final KeyValueBytesStoreSupplier supplier) {
        return new MaterializedX<>(configurator -> Materialized.as(supplier));
    }

    /**
     * @see Materialized#withKeySerde(Serde)
     */
    public MaterializedX<K, V, S> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify(
                (materialized, configurator) -> materialized.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Materialized#withKeySerde(Serde)
     */
    public MaterializedX<K, V, S> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Materialized#withValueSerde(Serde)
     */
    public MaterializedX<K, V, S> withValueSerde(final Preconfigured<? extends Serde<V>> valueSerde) {
        return this.modify((materialized, configurator) -> materialized.withValueSerde(
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Materialized#withValueSerde(Serde)
     */
    public MaterializedX<K, V, S> withValueSerde(final Serde<V> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Materialized#withRetention(Duration)
     */
    public MaterializedX<K, V, S> withRetention(final Duration retention) {
        return this.modify(materialized -> materialized.withRetention(retention));
    }

    /**
     * @see Materialized#withStoreType(DslStoreSuppliers)
     */
    public MaterializedX<K, V, S> withStoreType(final DslStoreSuppliers storeSuppliers) {
        return this.modify(materialized -> materialized.withStoreType(storeSuppliers));
    }

    /**
     * @see Materialized#withLoggingEnabled(Map)
     */
    public MaterializedX<K, V, S> withLoggingEnabled(final Map<String, String> config) {
        return this.modify(materialized -> materialized.withLoggingEnabled(config));
    }

    /**
     * @see Materialized#withLoggingDisabled()
     */
    public MaterializedX<K, V, S> withLoggingDisabled() {
        return this.modify(Materialized::withLoggingDisabled);
    }

    /**
     * @see Materialized#withCachingDisabled()
     */
    public MaterializedX<K, V, S> withCachingDisabled() {
        return this.modify(Materialized::withCachingDisabled);
    }

    /**
     * @see Materialized#withCachingEnabled()
     */
    public MaterializedX<K, V, S> withCachingEnabled() {
        return this.modify(Materialized::withCachingEnabled);
    }

    @Override
    protected MaterializedX<K, V, S> newInstance(final Function<Configurator, Materialized<K, V, S>> initializer) {
        return new MaterializedX<>(initializer);
    }
}
