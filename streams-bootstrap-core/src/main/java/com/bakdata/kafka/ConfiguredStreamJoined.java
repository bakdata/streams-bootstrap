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

import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.DslStoreSuppliers;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link StreamJoined} using {@link Configurator}
 * @param <K> type of keys
 * @param <V1> this value type
 * @param <V2> other value type
 * @see StreamJoined
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredStreamJoined<K, V1, V2> {

    @With
    private final @NonNull Preconfigured<Serde<K>> keySerde;
    @With
    private final @NonNull Preconfigured<Serde<V1>> valueSerde;
    @With
    private final @NonNull Preconfigured<Serde<V2>> otherValueSerde;
    @With
    private final DslStoreSuppliers dslStoreSuppliers;
    @With
    private final String name;
    @With
    private final String storeName;
    private final Map<String, String> topicConfig;
    private final boolean loggingEnabled;

    /**
     * Create an instance of {@code ConfiguredStreamJoined} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code ConfiguredStreamJoined}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> keySerde(
            final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde(), Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code ConfiguredStreamJoined} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code ConfiguredStreamJoined}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> valueSerde(
            final Preconfigured<Serde<V1>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code ConfiguredStreamJoined} with provided other value serde
     * @param valueSerde Serde to use for other values
     * @return a new instance of {@code ConfiguredStreamJoined}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> otherValueSerde(
            final Preconfigured<Serde<V2>> valueSerde) {
        return with(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * @see StreamJoined#with(Serde, Serde, Serde)
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> with(
            final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V1>> valueSerde,
            final Preconfigured<Serde<V2>> otherValueSerde) {
        return new ConfiguredStreamJoined<>(keySerde, valueSerde, otherValueSerde, null, null, null, new HashMap<>(),
                true);
    }

    /**
     * @see StreamJoined#as(String)
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> as(final String storeName) {
        return new ConfiguredStreamJoined<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(),
                Preconfigured.defaultSerde(),
                null, null, storeName, new HashMap<>(), true);
    }

    /**
     * @see StreamJoined#with(DslStoreSuppliers)
     */
    public static <K, V1, V2> ConfiguredStreamJoined<K, V1, V2> with(final DslStoreSuppliers storeSuppliers) {
        return new ConfiguredStreamJoined<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(),
                Preconfigured.defaultSerde(), storeSuppliers,
                null, null, new HashMap<>(), true);
    }

    StreamJoined<K, V1, V2> configure(final Configurator configurator) {
        final StreamJoined<K, V1, V2> streamJoined = StreamJoined.<K, V1, V2>as(this.storeName)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withOtherValueSerde(configurator.configureForValues(this.otherValueSerde))
                .withName(this.name)
                .withDslStoreSuppliers(this.dslStoreSuppliers);
        if (this.loggingEnabled) {
            return streamJoined.withLoggingEnabled(this.topicConfig);
        } else {
            return streamJoined.withLoggingDisabled();
        }
    }

}
