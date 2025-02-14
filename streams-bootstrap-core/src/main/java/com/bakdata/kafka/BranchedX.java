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

import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Extends the {@link org.apache.kafka.streams.kstream.Branched} interface by adding methods to simplify Serde
 * configuration, error handling,
 * and topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class BranchedX<K, V> {

    @Getter(AccessLevel.PROTECTED)
    private final String name;

    /**
     * @see Branched#withConsumer(Consumer)
     */
    public static <K, V> BranchedX<K, V> withConsumer(final Consumer<? super KStreamX<K, V>> chain) {
        return new ConsumerBranchedX<>(null, chain);
    }

    /**
     * @see Branched#withConsumer(Consumer, String)
     */
    public static <K, V> BranchedX<K, V> withConsumer(final Consumer<? super KStreamX<K, V>> chain, final String name) {
        return new ConsumerBranchedX<>(name, chain);
    }

    /**
     * @see Branched#withFunction(Function)
     */
    public static <K, V> BranchedX<K, V> withFunction(
            final Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain) {
        return new FunctionBranchedX<>(null, chain);
    }

    /**
     * @see Branched#withFunction(Function, String)
     */
    public static <K, V> BranchedX<K, V> withFunction(
            final Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain, final String name) {
        return new FunctionBranchedX<>(name, chain);
    }

    abstract Branched<K, V> convert(StreamsContext context);

    private static final class ConsumerBranchedX<K, V> extends BranchedX<K, V> {
        private final @NonNull Consumer<? super KStreamX<K, V>> chain;

        private ConsumerBranchedX(final String name, final Consumer<? super KStreamX<K, V>> chain) {
            super(name);
            this.chain = chain;
        }

        @Override
        Branched<K, V> convert(final StreamsContext context) {
            return Branched.withConsumer(this.asConsumer(context), this.getName());
        }

        private Consumer<KStream<K, V>> asConsumer(final StreamsContext context) {
            return stream -> this.chain.accept(context.wrap(stream));
        }
    }

    private static final class FunctionBranchedX<K, V> extends BranchedX<K, V> {
        private final @NonNull Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain;

        private FunctionBranchedX(final String name,
                final Function<? super KStreamX<K, V>, ? extends KStream<K, V>> chain) {
            super(name);
            this.chain = chain;
        }

        @Override
        Branched<K, V> convert(final StreamsContext context) {
            return Branched.withFunction(this.asFunction(context), this.getName());
        }

        private Function<KStream<K, V>, KStream<K, V>> asFunction(final StreamsContext context) {
            return stream -> this.chain.apply(context.wrap(stream));
        }
    }

}
