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
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.DslStoreSuppliers;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

/**
 * Use {@link Preconfigured} to lazily configure {@link Serde} for {@link StreamJoined} using {@link Configurator}
 * @param <K> type of keys
 * @param <V1> this value type
 * @param <V2> other value type
 * @see StreamJoined
 */
public final class StreamJoinedX<K, V1, V2> extends
        ModifierChain<StreamJoined<K, V1, V2>, Configurator, StreamJoinedX<K, V1, V2>> {

    private StreamJoinedX(final Function<Configurator, StreamJoined<K, V1, V2>> initializer) {
        super(initializer);
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> keySerde(
            final Preconfigured<? extends Serde<K>> keySerde) {
        return with(keySerde, Preconfigured.defaultSerde(), Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided key serde
     * @param keySerde Serde to use for keys
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> keySerde(final Serde<K> keySerde) {
        return keySerde(Preconfigured.create(keySerde));
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> valueSerde(
            final Preconfigured<? extends Serde<V1>> valueSerde) {
        return with(Preconfigured.defaultSerde(), valueSerde, Preconfigured.defaultSerde());
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided value serde
     * @param valueSerde Serde to use for values
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> valueSerde(final Serde<V1> valueSerde) {
        return valueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided other value serde
     * @param otherValueSerde Serde to use for other values
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> otherValueSerde(
            final Preconfigured<? extends Serde<V2>> otherValueSerde) {
        return with(Preconfigured.defaultSerde(), Preconfigured.defaultSerde(), otherValueSerde);
    }

    /**
     * Create an instance of {@code StreamJoinedX} with provided other value serde
     * @param otherValueSerde Serde to use for other values
     * @return a new instance of {@code StreamJoinedX}
     * @param <K> type of keys
     * @param <V1> this value type
     * @param <V2> other value type
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> otherValueSerde(final Serde<V2> otherValueSerde) {
        return otherValueSerde(Preconfigured.create(otherValueSerde));
    }

    /**
     * @see StreamJoined#with(Serde, Serde, Serde)
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> with(
            final Preconfigured<? extends Serde<K>> keySerde,
            final Preconfigured<? extends Serde<V1>> valueSerde,
            final Preconfigured<? extends Serde<V2>> otherValueSerde) {
        return new StreamJoinedX<>(configurator -> StreamJoined.with(configurator.configureForKeys(keySerde),
                configurator.configureForValues(valueSerde), configurator.configureForValues(otherValueSerde)));
    }

    /**
     * @see StreamJoined#with(Serde, Serde, Serde)
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> with(
            final Serde<K> keySerde,
            final Serde<V1> valueSerde,
            final Serde<V2> otherValueSerde) {
        return with(Preconfigured.create(keySerde), Preconfigured.create(valueSerde),
                Preconfigured.create(otherValueSerde));
    }

    /**
     * @see StreamJoined#as(String)
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> as(final String storeName) {
        return new StreamJoinedX<>(configurator -> StreamJoined.as(storeName));
    }

    /**
     * @see StreamJoined#with(DslStoreSuppliers)
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> with(final DslStoreSuppliers storeSuppliers) {
        return new StreamJoinedX<>(configurator -> StreamJoined.with(storeSuppliers));
    }

    /**
     * @see StreamJoined#with(WindowBytesStoreSupplier, WindowBytesStoreSupplier)
     */
    public static <K, V1, V2> StreamJoinedX<K, V1, V2> with(final WindowBytesStoreSupplier storeSupplier,
            final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoinedX<>(configurator -> StreamJoined.with(storeSupplier, otherStoreSupplier));
    }

    /**
     * @see StreamJoined#withKeySerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withKeySerde(final Preconfigured<? extends Serde<K>> keySerde) {
        return this.modify(
                (streamJoined, configurator) -> streamJoined.withKeySerde(configurator.configureForKeys(keySerde)));
    }

    /**
     * @see StreamJoined#withKeySerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withKeySerde(final Serde<K> keySerde) {
        return this.withKeySerde(Preconfigured.create(keySerde));
    }

    /**
     * @see StreamJoined#withValueSerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withValueSerde(final Preconfigured<? extends Serde<V1>> valueSerde) {
        return this.modify((streamJoined, configurator) -> streamJoined.withValueSerde(
                configurator.configureForValues(valueSerde)));
    }

    /**
     * @see StreamJoined#withValueSerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withValueSerde(final Serde<V1> valueSerde) {
        return this.withValueSerde(Preconfigured.create(valueSerde));
    }

    /**
     * @see StreamJoined#withOtherValueSerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withOtherValueSerde(final Preconfigured<Serde<V2>> otherValueSerde) {
        return this.modify((streamJoined, configurator) -> streamJoined.withOtherValueSerde(
                configurator.configureForValues(otherValueSerde)));
    }

    /**
     * @see StreamJoined#withOtherValueSerde(Serde)
     */
    public StreamJoinedX<K, V1, V2> withOtherValueSerde(final Serde<V2> otherValueSerde) {
        return this.withOtherValueSerde(Preconfigured.create(otherValueSerde));
    }

    /**
     * @see StreamJoined#withDslStoreSuppliers(DslStoreSuppliers)
     */
    public StreamJoinedX<K, V1, V2> withDslStoreSuppliers(final DslStoreSuppliers dslStoreSuppliers) {
        return this.modify(streamJoined -> streamJoined.withDslStoreSuppliers(dslStoreSuppliers));
    }

    /**
     * @see StreamJoined#withName(String)
     */
    public StreamJoinedX<K, V1, V2> withName(final String name) {
        return this.modify(streamJoined -> streamJoined.withName(name));
    }

    /**
     * @see StreamJoined#withStoreName(String)
     */
    public StreamJoinedX<K, V1, V2> withStoreName(final String storeName) {
        return this.modify(streamJoined -> streamJoined.withStoreName(storeName));
    }

    /**
     * @see StreamJoined#withLoggingEnabled(Map)
     */
    public StreamJoinedX<K, V1, V2> withLoggingEnabled(final Map<String, String> config) {
        return this.modify(streamJoined -> streamJoined.withLoggingEnabled(config));
    }

    /**
     * @see StreamJoined#withLoggingDisabled()
     */
    public StreamJoinedX<K, V1, V2> withLoggingDisabled() {
        return this.modify(StreamJoined::withLoggingDisabled);
    }

    /**
     * @see StreamJoined#withThisStoreSupplier(WindowBytesStoreSupplier)
     */
    public StreamJoinedX<K, V1, V2> withThisStoreSupplier(final WindowBytesStoreSupplier thisStoreSupplier) {
        return this.modify(streamJoined -> streamJoined.withThisStoreSupplier(thisStoreSupplier));
    }

    /**
     * @see StreamJoined#withOtherStoreSupplier(WindowBytesStoreSupplier)
     */
    public StreamJoinedX<K, V1, V2> withOtherStoreSupplier(final WindowBytesStoreSupplier otherStoreSupplier) {
        return this.modify(streamJoined -> streamJoined.withOtherStoreSupplier(otherStoreSupplier));
    }

    @Override
    protected StreamJoinedX<K, V1, V2> newInstance(final Function<Configurator, StreamJoined<K, V1, V2>> initializer) {
        return new StreamJoinedX<>(initializer);
    }

}
