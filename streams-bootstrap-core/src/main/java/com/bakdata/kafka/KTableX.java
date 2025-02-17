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

/**
 * Extends the {@link KTable} interface by adding methods to simplify Serde configuration, error handling, and topic
 * access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface KTableX<K, V> extends KTable<K, V> {

    @Override
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate);

    @Override
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #filter(Predicate, Materialized)
     */
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #filter(Predicate, Named, Materialized)
     */
    KTableX<K, V> filter(Predicate<? super K, ? super V> predicate, Named named,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate);

    @Override
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #filterNot(Predicate, Materialized)
     */
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named,
            Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #filterNot(Predicate, Named, Materialized)
     */
    KTableX<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named,
            MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #mapValues(ValueMapper, Materialized)
     */
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #mapValues(ValueMapper, Named, Materialized)
     */
    <VR> KTableX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #mapValues(ValueMapperWithKey, Materialized)
     */
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #mapValues(ValueMapperWithKey, Named, Materialized)
     */
    <VR> KTableX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KStreamX<K, V> toStream();

    @Override
    KStreamX<K, V> toStream(Named named);

    @Override
    <KR> KStreamX<KR, V> toStream(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);

    @Override
    <KR> KStreamX<KR, V> toStream(KeyValueMapper<? super K, ? super V, ? extends KR> mapper, Named named);

    @Override
    KTableX<K, V> suppress(Suppressed<? super K> suppressed);

    @Override
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, String... stateStoreNames);

    /**
     * @see #transformValues(ValueTransformerWithKeySupplier, Materialized, String...)
     */
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized, String... stateStoreNames);

    @Override
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, Named named, String... stateStoreNames);

    /**
     * @see #transformValues(ValueTransformerWithKeySupplier, Materialized, Named, String...)
     */
    <VR> KTableX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized, Named named,
            String... stateStoreNames);

    @Override
    <KR, VR> KGroupedTableX<KR, VR> groupBy(KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector);

    @Override
    <KR, VR> KGroupedTableX<KR, VR> groupBy(KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
            Grouped<KR, VR> grouped);

    /**
     * @see #groupBy(KeyValueMapper, Grouped)
     */
    <KR, VR> KGroupedTableX<KR, VR> groupBy(KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
            GroupedX<KR, VR> grouped);

    @Override
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named);

    @Override
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #join(KTable, ValueJoiner, Materialized)
     */
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #join(KTable, ValueJoiner, Named, Materialized)
     */
    <VO, VR> KTableX<K, VR> join(KTable<K, VO> other, ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named);

    @Override
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #leftJoin(KTable, ValueJoiner, Materialized)
     */
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #leftJoin(KTable, ValueJoiner, Named, Materialized)
     */
    <VO, VR> KTableX<K, VR> leftJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named);

    @Override
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     */
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #outerJoin(KTable, ValueJoiner, Named, Materialized)
     */
    <VO, VR> KTableX<K, VR> outerJoin(KTable<K, VO> other,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            Named named, MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner);

    @Deprecated
    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named);

    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined);

    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #join(KTable, Function, ValueJoiner, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Deprecated
    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #join(KTable, Function, ValueJoiner, Named, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #join(KTable, Function, ValueJoiner, TableJoined, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> join(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner);

    @Deprecated
    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named);

    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined);

    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #leftJoin(KTable, Function, ValueJoiner, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Deprecated
    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #leftJoin(KTable, Function, ValueJoiner, Named, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, Named named,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined,
            Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #leftJoin(KTable, Function, ValueJoiner, TableJoined, Materialized)
     */
    <VR, KO, VO> KTableX<K, VR> leftJoin(KTable<KO, VO> other, Function<V, KO> foreignKeyExtractor,
            ValueJoiner<V, VO, VR> joiner, TableJoined<K, KO> tableJoined,
            MaterializedX<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
}
