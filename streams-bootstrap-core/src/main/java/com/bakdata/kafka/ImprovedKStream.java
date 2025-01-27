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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public interface ImprovedKStream<K, V> extends KStream<K, V> {

    @Override
    ImprovedKStream<K, V> filter(Predicate<? super K, ? super V> predicate);

    @Override
    ImprovedKStream<K, V> filter(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    ImprovedKStream<K, V> filterNot(Predicate<? super K, ? super V> predicate);

    @Override
    ImprovedKStream<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    <KR> ImprovedKStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);

    @Override
    <KR> ImprovedKStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper, Named named);

    @Override
    <KR, VR> ImprovedKStream<KR, VR> map(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper);

    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper);

    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <KR, VR> ImprovedKStream<KR, VR> map(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named);

    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named);

    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> ImprovedKStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> ImprovedKStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper, Named named);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> ImprovedKStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> ImprovedKStream<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named);

    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <KR, VR> ImprovedKStream<KR, VR> flatMap(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <KR, VR> ImprovedKStream<KR, VR> flatMap(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            Named named);

    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            Named named);

    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> ImprovedKStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> ImprovedKStream<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> ImprovedKStream<K, VR> flatMapValues(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> ImprovedKStream<K, VR> flatMapValues(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper, Named named);

    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    ImprovedKStream<K, V> peek(ForeachAction<? super K, ? super V> action);

    @Override
    ImprovedKStream<K, V> peek(ForeachAction<? super K, ? super V> action, Named named);

    @Override
    ImprovedKStream<K, V>[] branch(Named named, Predicate<? super K, ? super V>... predicates);

    @Override
    ImprovedKStream<K, V>[] branch(Predicate<? super K, ? super V>... predicates);

    @Override
    ImprovedBranchedKStream<K, V> split();

    @Override
    ImprovedBranchedKStream<K, V> split(Named named);

    @Override
    ImprovedKStream<K, V> merge(KStream<K, V> stream);

    @Override
    ImprovedKStream<K, V> merge(KStream<K, V> stream, Named named);

    @Override
    ImprovedKStream<K, V> through(String topic);

    @Override
    ImprovedKStream<K, V> through(String topic, Produced<K, V> produced);

    @Override
    ImprovedKStream<K, V> repartition();

    @Override
    ImprovedKStream<K, V> repartition(Repartitioned<K, V> repartitioned);

    ImprovedKStream<K, V> repartition(ConfiguredRepartitioned<K, V> repartitioned);

    void toOutputTopic();

    void toOutputTopic(Produced<K, V> produced);

    void toOutputTopic(ConfiguredProduced<K, V> produced);

    void toOutputTopic(String label);

    void toOutputTopic(String label, Produced<K, V> produced);

    void toOutputTopic(String label, ConfiguredProduced<K, V> produced);

    void toErrorTopic();

    void toErrorTopic(Produced<K, V> produced);

    void toErrorTopic(ConfiguredProduced<K, V> produced);

    @Override
    ImprovedKTable<K, V> toTable();

    @Override
    ImprovedKTable<K, V> toTable(Named named);

    @Override
    ImprovedKTable<K, V> toTable(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    ImprovedKTable<K, V> toTable(ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    ImprovedKTable<K, V> toTable(Named named, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    ImprovedKTable<K, V> toTable(Named named, ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <KR> ImprovedKGroupedStream<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> keySelector);

    @Override
    <KR> ImprovedKGroupedStream<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> keySelector,
            Grouped<KR, V> grouped);

    @Override
    ImprovedKGroupedStream<K, V> groupByKey();

    @Override
    ImprovedKGroupedStream<K, V> groupByKey(Grouped<K, V> grouped);

    @Override
    <VO, VR> ImprovedKStream<K, VR> join(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> join(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> join(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            JoinWindows windows, StreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> ImprovedKStream<K, VR> join(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> ImprovedKStream<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> ImprovedKStream<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> ImprovedKStream<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> ImprovedKStream<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> ImprovedKStream<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    @Override
    <VT, VR> ImprovedKStream<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> ImprovedKStream<K, VR> join(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> ImprovedKStream<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            Joined<K, V, VT> joined);

    @Override
    <VT, VR> ImprovedKStream<K, VR> join(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined);

    @Override
    <VT, VR> ImprovedKStream<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> ImprovedKStream<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> ImprovedKStream<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            Joined<K, V, VT> joined);

    @Override
    <VT, VR> ImprovedKStream<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> joiner);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> joiner, Named named);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner, Named named);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner, Named named);

    @Override
    <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner, Named named);

    @Override
    <K1, V1> ImprovedKStream<K1, V1> transform(
            TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
            String... stateStoreNames);

    @Override
    <K1, V1> ImprovedKStream<K1, V1> transform(
            TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <K1, V1> ImprovedKStream<K1, V1> flatTransform(
            TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
            String... stateStoreNames);

    @Override
    <K1, V1> ImprovedKStream<K1, V1> flatTransform(
            TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> transformValues(
            ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> transformValues(
            ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> flatTransformValues(
            ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> flatTransformValues(
            ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> flatTransformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> ImprovedKStream<K, VR> flatTransformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <KOut, VOut> ImprovedKStream<KOut, VOut> process(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            String... stateStoreNames);

    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            String... stateStoreNames);

    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            String... stateStoreNames);

    @Override
    <KOut, VOut> ImprovedKStream<KOut, VOut> process(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            Named named, String... stateStoreNames);

    @Override
    <VOut> ImprovedKStream<K, VOut> processValues(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            String... stateStoreNames);

    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            String... stateStoreNames);

    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            String... stateStoreNames);

    @Override
    <VOut> ImprovedKStream<K, VOut> processValues(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            Named named, String... stateStoreNames);
}
