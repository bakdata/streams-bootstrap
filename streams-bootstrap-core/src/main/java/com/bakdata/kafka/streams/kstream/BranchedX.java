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

import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Extends the {@link org.apache.kafka.streams.kstream.Branched} interface by adding methods to simplify Serde
 * configuration, error handling,
 * and topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
public final class BranchedX<K, V> extends ModifierChain<Branched<K, V>, StreamsContext, BranchedX<K, V>> {

    private BranchedX(final Function<StreamsContext, Branched<K, V>> initializer) {
        super(initializer);
    }

    /**
     * @see Branched#withConsumer(Consumer)
     */
    public static <K, V> BranchedX<K, V> withConsumer(final Consumer<? super KStreamX<K, V>> chain) {
        return new BranchedX<>(context -> Branched.withConsumer(asConsumer(chain, context)));
    }

    /**
     * @see Branched#withFunction(Function)
     */
    public static <K, V> BranchedX<K, V> withFunction(
            final Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain) {
        return new BranchedX<>(context -> Branched.withFunction(asFunction(chain, context)));
    }

    /**
     * @see Branched#as(String)
     */
    public static <K, V> BranchedX<K, V> as(final String name) {
        return new BranchedX<>(context -> Branched.as(name));
    }

    private static <K, V> Consumer<KStream<K, V>> asConsumer(final Consumer<? super KStreamX<K, V>> chain,
            final StreamsContext context) {
        return stream -> chain.accept(context.wrap(stream));
    }

    private static <K, V> Function<KStream<K, V>, KStream<K, V>> asFunction(
            final Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain, final StreamsContext context) {
        return stream -> chain.apply(context.wrap(stream));
    }

    /**
     * @see Branched#withName(String)
     */
    public BranchedX<K, V> withName(final String name) {
        return this.modify(branched -> branched.withName(name));
    }

    @Override
    protected BranchedX<K, V> newInstance(final Function<StreamsContext, Branched<K, V>> initializer) {
        return new BranchedX<>(initializer);
    }
}
