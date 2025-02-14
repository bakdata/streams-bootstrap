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

import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class KTableXImpl<K, V> implements KTableX<K, V> {

    @Getter(AccessLevel.PROTECTED)
    private final @NonNull KTable<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filter(predicate));
    }

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filter(predicate, named));
    }

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.filter(predicate, materialized));
    }

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.filter(predicate, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.filter(predicate, named, materialized));
    }

    @Override
    public KTableX<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.filter(predicate, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filterNot(predicate));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filterNot(predicate, named));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.filterNot(predicate, materialized));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.filterNot(predicate, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.filterNot(predicate, materialized));
    }

    @Override
    public KTableX<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named,
            final AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.filterNot(predicate, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.mapValues(mapper, materialized));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.mapValues(mapper, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named, materialized));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.mapValues(mapper, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.mapValues(mapper, materialized));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.mapValues(mapper, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Named named, final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named, materialized));
    }

    @Override
    public <VR> KTableX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Named named, final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.mapValues(mapper, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KStreamX<K, V> toStream() {
        return this.context.wrap(this.wrapped.toStream());
    }

    @Override
    public KStreamX<K, V> toStream(final Named named) {
        return this.context.wrap(this.wrapped.toStream(named));
    }

    @Override
    public <KR> KStreamX<KR, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        return this.context.wrap(this.wrapped.toStream(mapper));
    }

    @Override
    public <KR> KStreamX<KR, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.toStream(mapper, named));
    }

    @Override
    public KTableX<K, V> suppress(final Suppressed<? super K> suppressed) {
        return this.context.wrap(this.wrapped.suppress(suppressed));
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(transformerSupplier, stateStoreNames));
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(transformerSupplier, named, stateStoreNames));
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(transformerSupplier, materialized, stateStoreNames));
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
            final String... stateStoreNames) {
        return this.transformValues(transformerSupplier, materialized.configure(this.context.getConfigurator()),
                stateStoreNames);
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(
                this.wrapped.transformValues(transformerSupplier, materialized, named, stateStoreNames));
    }

    @Override
    public <VR> KTableX<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, final Named named,
            final String... stateStoreNames) {
        return this.transformValues(transformerSupplier, materialized.configure(this.context.getConfigurator()), named,
                stateStoreNames);
    }

    @Override
    public <KR, VR> KGroupedTableX<KR, VR> groupBy(
            final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector) {
        return this.context.wrap(this.wrapped.groupBy(selector));
    }

    @Override
    public <KR, VR> KGroupedTableX<KR, VR> groupBy(
            final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector, final Grouped<KR, VR> grouped) {
        return this.context.wrap(this.wrapped.groupBy(selector, grouped));
    }

    @Override
    public <KR, VR> KGroupedTableX<KR, VR> groupBy(
            final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector, final AutoGrouped<KR, VR> grouped) {
        return this.groupBy(selector, grouped.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, joiner));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, joiner, named));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.join(other, joiner, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> join(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.join(other, joiner, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, joiner));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, joiner, named));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.leftJoin(other, joiner, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> leftJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.leftJoin(other, joiner, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.outerJoin(otherTable, joiner));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.outerJoin(otherTable, joiner, named));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.outerJoin(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.outerJoin(other, joiner, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<K, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.outerJoin(otherTable, joiner, materialized));
    }

    @Override
    public <VO, VR> KTableX<K, VR> outerJoin(final KTable<K, VO> other,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.outerJoin(other, joiner, named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner, named));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner, tableJoined));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.join(other, foreignKeyExtractor, joiner, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner, named, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.join(other, foreignKeyExtractor, joiner, named,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.join(otherTable, foreignKeyExtractor, joiner, tableJoined, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> join(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.join(other, foreignKeyExtractor, joiner, tableJoined,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner, named));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner, tableJoined));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.leftJoin(other, foreignKeyExtractor, joiner,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner, named, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final Named named,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.leftJoin(other, foreignKeyExtractor, joiner, named,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined,
            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final KTable<KO, VO> otherTable = StreamsContext.maybeUnwrap(other);
        return this.context.wrap(
                this.wrapped.leftJoin(otherTable, foreignKeyExtractor, joiner, tableJoined, materialized));
    }

    @Override
    public <VR, KO, VO> KTableX<K, VR> leftJoin(final KTable<KO, VO> other,
            final Function<V, KO> foreignKeyExtractor,
            final ValueJoiner<V, VO, VR> joiner, final TableJoined<K, KO> tableJoined,
            final AutoMaterialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.leftJoin(other, foreignKeyExtractor, joiner, tableJoined,
                materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public String queryableStoreName() {
        return this.wrapped.queryableStoreName();
    }
}
