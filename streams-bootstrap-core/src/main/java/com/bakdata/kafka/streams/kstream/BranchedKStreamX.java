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

import java.util.Map;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

/**
 * Extends the {@link BranchedKStream} interface by adding methods to simplify Serde configuration, error handling,
 * and topic access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface BranchedKStreamX<K, V> extends BranchedKStream<K, V> {

    @Override
    BranchedKStreamX<K, V> branch(Predicate<? super K, ? super V> predicate);

    @Override
    BranchedKStreamX<K, V> branch(Predicate<? super K, ? super V> predicate, Branched<K, V> branched);

    /**
     * @see #branch(Predicate, Branched)
     */
    BranchedKStreamX<K, V> branch(Predicate<? super K, ? super V> predicate, BranchedX<K, V> branched);

    /**
     * @deprecated Use {@link #defaultBranchX()} instead.
     */
    @Deprecated(since = "4.0.0")
    @Override
    Map<String, KStream<K, V>> defaultBranch();

    /**
     * @see BranchedKStream#defaultBranch()
     */
    Map<String, KStreamX<K, V>> defaultBranchX();

    /**
     * @deprecated Use {@link #defaultBranchX(Branched)} instead.
     */
    @Deprecated(since = "4.0.0")
    @Override
    Map<String, KStream<K, V>> defaultBranch(Branched<K, V> branched);

    /**
     * @see BranchedKStream#defaultBranch(Branched)
     */
    Map<String, KStreamX<K, V>> defaultBranchX(Branched<K, V> branched);

    /**
     * @see #defaultBranch(Branched)
     */
    Map<String, KStreamX<K, V>> defaultBranch(BranchedX<K, V> branched);

    /**
     * @deprecated Use {@link #noDefaultBranchX()} instead.
     */
    @Deprecated(since = "4.0.0")
    @Override
    Map<String, KStream<K, V>> noDefaultBranch();

    /**
     * @see BranchedKStream#noDefaultBranch()
     */
    Map<String, KStreamX<K, V>> noDefaultBranchX();
}
