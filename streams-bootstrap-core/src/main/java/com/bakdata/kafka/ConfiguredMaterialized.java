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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.DslStoreSuppliers;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredMaterialized<K, V, S extends StateStore> {

    @With
    private final @NonNull Preconfigured<Serde<K>> keySerde;
    @With
    private final @NonNull Preconfigured<Serde<V>> valueSerde;
    private final String storeName;
    @With
    private final Duration retention;
    @With
    private final DslStoreSuppliers storeType;
    private final Map<String, String> topicConfig;
    private final boolean loggingEnabled;
    private final boolean cachingEnabled;

    public static <K, V, S extends StateStore> ConfiguredMaterialized<K, V, S> keySerde(
            final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde());
    }

    public static <K, V, S extends StateStore> ConfiguredMaterialized<K, V, S> valueSerde(
            final Preconfigured<Serde<V>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde);
    }

    public static <K, V, S extends StateStore> ConfiguredMaterialized<K, V, S> with(
            final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V>> valueSerde) {
        return new ConfiguredMaterialized<>(keySerde, valueSerde, null, null, null, new HashMap<>(), true, true);
    }

    public static <K, V, S extends StateStore> ConfiguredMaterialized<K, V, S> as(final String storeName) {
        return new ConfiguredMaterialized<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), storeName, null,
                null, new HashMap<>(), true, true);
    }

    public static <K, V, S extends StateStore> ConfiguredMaterialized<K, V, S> as(
            final DslStoreSuppliers storeSuppliers) {
        return new ConfiguredMaterialized<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), null, null,
                storeSuppliers, new HashMap<>(), true, true);
    }

    Materialized<K, V, S> configure(final Configurator configurator) {
        final Materialized<K, V, S> materialized = Materialized.<K, V, S>as(this.storeName)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde));
        if (this.retention != null) {
            materialized.withRetention(this.retention);
        }
        if (this.storeType != null) {
            materialized.withStoreType(this.storeType);
        }
        if (this.loggingEnabled) {
            materialized.withLoggingEnabled(this.topicConfig);
        } else {
            materialized.withLoggingDisabled();
        }
        if (this.cachingEnabled) {
            materialized.withCachingEnabled();
        } else {
            materialized.withCachingDisabled();
        }
        return materialized;
    }

}
