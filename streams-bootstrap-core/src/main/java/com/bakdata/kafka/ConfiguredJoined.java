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
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.With;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Joined} using {@link Configurator}
 * @param <K> type of keys
 * @param <V1> this value type
 * @param <V2> other value type
 * @see Joined
 */
@With
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConfiguredJoined<K, V1, V2> {

    private final @NonNull Preconfigured<Serde<K>> keySerde;
    private final @NonNull Preconfigured<Serde<V1>> valueSerde;
    private final @NonNull Preconfigured<Serde<V2>> otherValueSerde;
    private final String name;
    private final Duration gracePeriod;

    /**
     * @see Joined#keySerde(Serde) 
     */
    public static <K, V1, V2> ConfiguredJoined<K, V1, V2> keySerde(
            final Preconfigured<Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde(), Preconfigured.defaultSerde());
    }

    /**
     * @see Joined#valueSerde(Serde) 
     */
    public static <K, V1, V2> ConfiguredJoined<K, V1, V2> valueSerde(
            final Preconfigured<Serde<V1>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde, Preconfigured.defaultSerde());
    }

    /**
     * @see Joined#otherValueSerde(Serde) 
     */
    public static <K, V1, V2> ConfiguredJoined<K, V1, V2> otherValueSerde(
            final Preconfigured<Serde<V2>> valueSerde) {
        return with(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), valueSerde);
    }

    /**
     * @see Joined#with(Serde, Serde, Serde) 
     */
    public static <K, V1, V2> ConfiguredJoined<K, V1, V2> with(
            final Preconfigured<Serde<K>> keySerde,
            final Preconfigured<Serde<V1>> valueSerde,
            final Preconfigured<Serde<V2>> otherValueSerde) {
        return new ConfiguredJoined<>(keySerde, valueSerde, otherValueSerde, null, null);
    }

    /**
     * @see Joined#as(String) 
     */
    public static <K, V1, V2> ConfiguredJoined<K, V1, V2> as(final String name) {
        return new ConfiguredJoined<>(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(),
                Preconfigured.defaultSerde(), name, null);
    }

    Joined<K, V1, V2> configure(final Configurator configurator) {
        return Joined.<K, V1, V2>as(this.name)
                .withKeySerde(configurator.configureForKeys(this.keySerde))
                .withValueSerde(configurator.configureForValues(this.valueSerde))
                .withOtherValueSerde(configurator.configureForValues(this.otherValueSerde))
                .withName(this.name)
                .withGracePeriod(this.gracePeriod);
    }

}
