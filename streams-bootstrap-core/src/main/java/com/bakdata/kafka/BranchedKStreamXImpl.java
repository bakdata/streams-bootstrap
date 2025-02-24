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

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

@RequiredArgsConstructor
class BranchedKStreamXImpl<K, V> implements BranchedKStreamX<K, V> {

    private final @NonNull BranchedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    private static <K, V, T> Map<String, T> wrap(
            final Map<String, ? extends KStream<K, V>> streamMap,
            final Function<? super Entry<String, ? extends KStream<K, V>>, ? extends T> wrapValue) {
        return streamMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, wrapValue));
    }

    @Override
    public BranchedKStreamX<K, V> branch(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.branch(predicate));
    }

    @Override
    public BranchedKStreamX<K, V> branch(final Predicate<? super K, ? super V> predicate,
            final Branched<K, V> branched) {
        return this.context.wrap(this.wrapped.branch(predicate, branched));
    }

    @Override
    public BranchedKStreamX<K, V> branch(final Predicate<? super K, ? super V> predicate,
            final BranchedX<K, V> branched) {
        return this.branch(predicate, branched.configure(this.context));
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch() {
        return this.wrap(this.defaultBranchInternal());
    }

    @Override
    public Map<String, KStreamX<K, V>> defaultBranchX() {
        return this.wrapX(this.defaultBranchInternal());
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch(final Branched<K, V> branched) {
        return this.wrap(this.defaultBranchInternal(branched));
    }

    @Override
    public Map<String, KStreamX<K, V>> defaultBranchX(final Branched<K, V> branched) {
        return this.wrapX(this.defaultBranchInternal(branched));
    }

    @Override
    public Map<String, KStreamX<K, V>> defaultBranch(final BranchedX<K, V> branched) {
        return this.wrapX(this.defaultBranchInternal(branched.configure(this.context)));
    }

    @Override
    public Map<String, KStream<K, V>> noDefaultBranch() {
        return this.wrap(this.noDefaultBranchInternal());
    }

    @Override
    public Map<String, KStreamX<K, V>> noDefaultBranchX() {
        return this.wrapX(this.noDefaultBranchInternal());
    }

    private Map<String, KStream<K, V>> noDefaultBranchInternal() {
        return this.wrapped.noDefaultBranch();
    }

    private Map<String, KStream<K, V>> defaultBranchInternal() {
        return this.wrapped.defaultBranch();
    }

    private Map<String, KStream<K, V>> defaultBranchInternal(final Branched<K, V> branched) {
        return this.wrapped.defaultBranch(branched);
    }

    private Map<String, KStream<K, V>> wrap(final Map<String, ? extends KStream<K, V>> streamMap) {
        return BranchedKStreamXImpl.<K, V, KStream<K, V>>wrap(streamMap, this::wrapValue);
    }

    private Map<String, KStreamX<K, V>> wrapX(final Map<String, ? extends KStream<K, V>> streamMap) {
        return BranchedKStreamXImpl.<K, V, KStreamX<K, V>>wrap(streamMap, this::wrapValue);
    }

    private KStreamX<K, V> wrapValue(final Entry<String, ? extends KStream<K, V>> entry) {
        return this.context.wrap(entry.getValue());
    }
}
