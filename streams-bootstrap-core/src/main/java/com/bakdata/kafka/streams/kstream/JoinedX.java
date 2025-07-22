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
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link Joined} using {@link Configurator}
 * @param <K> type of keys
 * @param <V1> this value type
 * @param <V2> other value type
 * @see Joined
 */
public final class JoinedX<K, V1, V2> extends ModifierChain<Joined<K, V1, V2>, Configurator, JoinedX<K, V1, V2>> {

    private JoinedX(final Function<Configurator, Joined<K, V1, V2>> initializer) {
        super(initializer);
    }

    /**
     * @see Joined#keySerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> keySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return new JoinedX<>(configurator -> Joined.keySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Joined#keySerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Joined#valueSerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> valueSerde(final Preconfigured<? extends Serde<V1>> valueSerde) {
        return new JoinedX<>(configurator -> Joined.valueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Joined#valueSerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> valueSerde(final Serde<V1> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Joined#otherValueSerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> otherValueSerde(
            final Preconfigured<? extends Serde<V2>> otherValueSerde) {
        return new JoinedX<>(configurator -> Joined.otherValueSerde(configurator.configureForValues(otherValueSerde)));
    }

    /**
     * @see Joined#otherValueSerde(Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> otherValueSerde(final Serde<V2> otherValueSerde) {
        return otherValueSerde(Preconfigured.create(otherValueSerde));
    }

    /**
     * @see Joined#with(Serde, Serde, Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> with(
            final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V1>> valueSerde,
            final Preconfigured<? extends Serde<V2>> otherValueSerde) {
        return new JoinedX<>(configurator -> Joined.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde), configurator.configureForValues(otherValueSerde)));
    }

    /**
     * @see Joined#with(Serde, Serde, Serde)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> with(
            final Serde<K> keySerde,
            final Serde<V1> valueSerde,
            final Serde<V2> otherValueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde),
                Preconfigured.create(otherValueSerde));
    }

    /**
     * @see Joined#as(String)
     */
    public static <K, V1, V2> JoinedX<K, V1, V2> as(final String name) {
        return new JoinedX<>(configurator -> Joined.as(name));
    }

    /**
     * @see Joined#withKeySerde(Serde)
     */
    public JoinedX<K, V1, V2> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify((joined, configurator) -> joined.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see Joined#withKeySerde(Serde)
     */
    public JoinedX<K, V1, V2> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see Joined#withValueSerde(Serde)
     */
    public JoinedX<K, V1, V2> withValueSerde(final Preconfigured<? extends Serde<V1>> valueSerde) {
        return this.modify(
                (joined, configurator) -> joined.withValueSerde(configurator.configureForValues(valueSerde)));
    }

    /**
     * @see Joined#withValueSerde(Serde)
     */
    public JoinedX<K, V1, V2> withValueSerde(final Serde<V1> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see Joined#withOtherValueSerde(Serde)
     */
    public JoinedX<K, V1, V2> withOtherValueSerde(final Preconfigured<? extends Serde<V2>> otherValueSerde) {
        return this.modify(
                (joined, configurator) -> joined.withOtherValueSerde(configurator.configureForValues(otherValueSerde)));
    }

    /**
     * @see Joined#withOtherValueSerde(Serde)
     */
    public JoinedX<K, V1, V2> withOtherValueSerde(final Serde<V2> otherValueSerde) {
        return this.withOtherValueSerde(Preconfigured.create(otherValueSerde));
    }

    /**
     * @see Joined#withName(String)
     */
    public JoinedX<K, V1, V2> withName(final String name) {
        return this.modify(joined -> joined.withName(name));
    }

    /**
     * @see Joined#withGracePeriod(Duration)
     */
    public JoinedX<K, V1, V2> withGracePeriod(final Duration gracePeriod) {
        return this.modify(joined -> joined.withGracePeriod(gracePeriod));
    }

    @Override
    protected JoinedX<K, V1, V2> newInstance(final Function<Configurator, Joined<K, V1, V2>> initializer) {
        return new JoinedX<>(initializer);
    }
}
