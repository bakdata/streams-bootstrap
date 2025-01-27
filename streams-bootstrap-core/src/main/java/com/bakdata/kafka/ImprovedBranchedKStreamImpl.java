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
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

@RequiredArgsConstructor
public class ImprovedBranchedKStreamImpl<K, V> implements ImprovedBranchedKStream<K, V> {

    private final @NonNull BranchedKStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public ImprovedBranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.branch(predicate));
    }

    @Override
    public ImprovedBranchedKStream<K, V> branch(final Predicate<? super K, ? super V> predicate,
            final Branched<K, V> branched) {
        return this.context.wrap(this.wrapped.branch(predicate, branched));
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch() {
        return this.wrap(this.wrapped.defaultBranch());
    }

    @Override
    public Map<String, KStream<K, V>> defaultBranch(final Branched<K, V> branched) {
        return this.wrap(this.wrapped.defaultBranch(branched));
    }

    @Override
    public Map<String, KStream<K, V>> noDefaultBranch() {
        return this.wrap(this.wrapped.noDefaultBranch());
    }

    private Map<String, KStream<K, V>> wrap(final Map<String, ? extends KStream<K, V>> streamMap) {
        return streamMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey, this::wrapValue));
    }

    private ImprovedKStream<K, V> wrapValue(final Entry<String, ? extends KStream<K, V>> entry) {
        return this.context.wrap(entry.getValue());
    }
}
