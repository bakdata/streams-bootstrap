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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedWindowStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Stores} using {@link Configurator}
 */
@RequiredArgsConstructor
public class ConfiguredStores {

    private final @NonNull Configurator configurator;

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code SessionStore}
     * @param supplier a {@code SessionBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#sessionStoreBuilder(SessionBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<SessionStore<K, V>> sessionStoreBuilder(final SessionBytesStoreSupplier supplier,
            final Preconfigured<? extends Serde<K>> keySerde, final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.sessionStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code TimestampedWindowStore}
     * @param supplier a {@code WindowBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#timestampedWindowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<TimestampedWindowStore<K, V>> timestampedWindowStoreBuilder(
            final WindowBytesStoreSupplier supplier, final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.timestampedWindowStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code WindowStore}
     * @param supplier a {@code WindowBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#windowStoreBuilder(WindowBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<WindowStore<K, V>> windowStoreBuilder(final WindowBytesStoreSupplier supplier,
            final Preconfigured<? extends Serde<K>> keySerde, final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.windowStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code VersionedKeyValueStore}
     * @param supplier a {@code VersionedBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#versionedKeyValueStoreBuilder(VersionedBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<VersionedKeyValueStore<K, V>> versionedKeyValueStoreBuilder(
            final VersionedBytesStoreSupplier supplier, final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.versionedKeyValueStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code TimestampedKeyValueStore}
     * @param supplier a {@code KeyValueBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#timestampedKeyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<TimestampedKeyValueStore<K, V>> timestampedKeyValueStoreBuilder(
            final KeyValueBytesStoreSupplier supplier, final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.timestampedKeyValueStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }

    /**
     * Creates a {@code StoreBuilder} that can be used to build a {@code KeyValueStore}
     * @param supplier a {@code KeyValueBytesStoreSupplier}
     * @param keySerde the key serde to use
     * @param valueSerde the value serde to use
     * @return a store builder
     * @param <K> key type
     * @param <V> value type
     * @see Stores#keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde)
     */
    public <K, V> StoreBuilder<KeyValueStore<K, V>> keyValueStoreBuilder(final KeyValueBytesStoreSupplier supplier,
            final Preconfigured<? extends Serde<K>> keySerde, final Preconfigured<? extends Serde<V>> valueSerde) {
        return Stores.keyValueStoreBuilder(supplier, this.configurator.configureForKeys(keySerde),
                this.configurator.configureForValues(valueSerde));
    }
}
