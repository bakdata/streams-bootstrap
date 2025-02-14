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

import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
import org.apache.kafka.streams.kstream.Printed;
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
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class ImprovedKStreamImpl<K, V> implements ImprovedKStream<K, V> {

    @Getter(AccessLevel.PACKAGE)
    private final @NonNull KStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public ImprovedKStream<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filter(predicate));
    }

    @Override
    public ImprovedKStream<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filter(predicate, named));
    }

    @Override
    public ImprovedKStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filterNot(predicate));
    }

    @Override
    public ImprovedKStream<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filterNot(predicate, named));
    }

    @Override
    public <KR> ImprovedKStream<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        return this.context.wrap(this.wrapped.selectKey(mapper));
    }

    @Override
    public <KR> ImprovedKStream<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.selectKey(mapper, named));
    }

    @Override
    public <KR, VR> ImprovedKStream<KR, VR> map(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.map(mapper));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <KR, VR> ImprovedKStream<KR, VR> map(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.map(mapper, named));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final Named named) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper, errorFilter), named);
    }

    @Override
    public <VR> ImprovedKStream<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends VR> mapper) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> ImprovedKStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapperWithKey.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapValuesCapturingErrorsInternal(
                ErrorCapturingValueMapperWithKey.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapperWithKey.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapValuesCapturingErrorsInternal(
                ErrorCapturingValueMapperWithKey.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <KR, VR> ImprovedKStream<KR, VR> flatMap(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper) {
        return this.context.wrap(this.wrapped.flatMap(mapper));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <KR, VR> ImprovedKStream<KR, VR> flatMap(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMap(mapper, named));
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final Named named) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatMapValues(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapper.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatMapValues(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper, final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter,
            final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatMapValues(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatMapValues(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper),
                named);
    }

    @Override
    public <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper, errorFilter), named);
    }

    @Override
    public void print(final Printed<K, V> printed) {
        this.wrapped.print(printed);
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action) {
        this.wrapped.foreach(action);
    }

    @Override
    public void foreach(final ForeachAction<? super K, ? super V> action, final Named named) {
        this.wrapped.foreach(action, named);
    }

    @Override
    public ImprovedKStream<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        return this.context.wrap(this.wrapped.peek(action));
    }

    @Override
    public ImprovedKStream<K, V> peek(final ForeachAction<? super K, ? super V> action, final Named named) {
        return this.context.wrap(this.wrapped.peek(action, named));
    }

    @Override
    public ImprovedKStream<K, V>[] branch(final Predicate<? super K, ? super V>... predicates) {
        return Arrays.stream(this.wrapped.branch(predicates))
                .map(this.context::wrap)
                .toArray(ImprovedKStream[]::new);
    }

    @Override
    public ImprovedKStream<K, V>[] branch(final Named named, final Predicate<? super K, ? super V>... predicates) {
        return Arrays.stream(this.wrapped.branch(named, predicates))
                .map(this.context::wrap)
                .toArray(ImprovedKStream[]::new);
    }

    @Override
    public ImprovedBranchedKStream<K, V> split() {
        return this.context.wrap(this.wrapped.split());
    }

    @Override
    public ImprovedBranchedKStream<K, V> split(final Named named) {
        return this.context.wrap(this.wrapped.split(named));
    }

    @Override
    public ImprovedKStream<K, V> merge(final KStream<K, V> stream) {
        final KStream<K, V> other = StreamsContext.maybeUnwrap(stream);
        return this.context.wrap(this.wrapped.merge(other));
    }

    @Override
    public ImprovedKStream<K, V> merge(final KStream<K, V> stream, final Named named) {
        final KStream<K, V> other = StreamsContext.maybeUnwrap(stream);
        return this.context.wrap(this.wrapped.merge(other, named));
    }

    @Override
    public ImprovedKStream<K, V> through(final String topic) {
        return this.context.wrap(this.wrapped.through(topic));
    }

    @Override
    public ImprovedKStream<K, V> through(final String topic, final Produced<K, V> produced) {
        return this.context.wrap(this.wrapped.through(topic, produced));
    }

    @Override
    public ImprovedKStream<K, V> repartition() {
        return this.context.wrap(this.wrapped.repartition());
    }

    @Override
    public ImprovedKStream<K, V> repartition(final Repartitioned<K, V> repartitioned) {
        return this.context.wrap(this.wrapped.repartition(repartitioned));
    }

    @Override
    public ImprovedKStream<K, V> repartition(final ConfiguredRepartitioned<K, V> repartitioned) {
        return this.repartition(repartitioned.configure(this.context.getConfigurator()));
    }

    @Override
    public void to(final String topic) {
        this.wrapped.to(topic);
    }

    @Override
    public void to(final String topic, final Produced<K, V> produced) {
        this.wrapped.to(topic, produced);
    }

    @Override
    public void to(final String topic, final ConfiguredProduced<K, V> produced) {
        this.to(topic, produced.configure(this.context.getConfigurator()));
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor) {
        this.wrapped.to(topicExtractor);
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor, final Produced<K, V> produced) {
        this.wrapped.to(topicExtractor, produced);
    }

    @Override
    public void to(final TopicNameExtractor<K, V> topicExtractor, final ConfiguredProduced<K, V> produced) {
        this.to(topicExtractor, produced.configure(this.context.getConfigurator()));
    }

    @Override
    public void toOutputTopic() {
        this.to(this.context.getTopics().getOutputTopic());
    }

    @Override
    public void toOutputTopic(final Produced<K, V> produced) {
        this.to(this.context.getTopics().getOutputTopic(), produced);
    }

    @Override
    public void toOutputTopic(final ConfiguredProduced<K, V> produced) {
        this.toOutputTopic(produced.configure(this.context.getConfigurator()));
    }

    @Override
    public void toOutputTopic(final String label) {
        this.to(this.context.getTopics().getOutputTopic(label));
    }

    @Override
    public void toOutputTopic(final String label, final Produced<K, V> produced) {
        this.to(this.context.getTopics().getOutputTopic(label), produced);
    }

    @Override
    public void toOutputTopic(final String label, final ConfiguredProduced<K, V> produced) {
        this.toOutputTopic(label, produced.configure(this.context.getConfigurator()));
    }

    @Override
    public void toErrorTopic() {
        this.to(this.context.getTopics().getErrorTopic());
    }

    @Override
    public void toErrorTopic(final Produced<K, V> produced) {
        this.to(this.context.getTopics().getErrorTopic(), produced);
    }

    @Override
    public void toErrorTopic(final ConfiguredProduced<K, V> produced) {
        this.toErrorTopic(produced.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<K, V> toTable() {
        return this.context.wrap(this.wrapped.toTable());
    }

    @Override
    public ImprovedKTable<K, V> toTable(final Named named) {
        return this.context.wrap(this.wrapped.toTable(named));
    }

    @Override
    public ImprovedKTable<K, V> toTable(final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.toTable(materialized));
    }

    @Override
    public ImprovedKTable<K, V> toTable(final ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.toTable(materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKTable<K, V> toTable(final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.toTable(named, materialized));
    }

    @Override
    public ImprovedKTable<K, V> toTable(final Named named,
            final ConfiguredMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.toTable(named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <KR> ImprovedKGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector) {
        return this.context.wrap(this.wrapped.groupBy(keySelector));
    }

    @Override
    public <KR> ImprovedKGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector,
            final Grouped<KR, V> grouped) {
        return this.context.wrap(this.wrapped.groupBy(keySelector, grouped));
    }

    @Override
    public <KR> ImprovedKGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector,
            final ConfiguredGrouped<KR, V> grouped) {
        return this.groupBy(keySelector, grouped.configure(this.context.getConfigurator()));
    }

    @Override
    public ImprovedKGroupedStream<K, V> groupByKey() {
        return this.context.wrap(this.wrapped.groupByKey());
    }

    @Override
    public ImprovedKGroupedStream<K, V> groupByKey(final Grouped<K, V> grouped) {
        return this.context.wrap(this.wrapped.groupByKey(grouped));
    }

    @Override
    public ImprovedKGroupedStream<K, V> groupByKey(final ConfiguredGrouped<K, V> grouped) {
        return this.groupByKey(grouped.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.join(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.join(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.leftJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.leftJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.outerJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> ImprovedKStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final ConfiguredStreamJoined<K, V, VO> streamJoined) {
        return this.outerJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner, joined));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final ConfiguredJoined<K, V, VT> joined) {
        return this.join(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner, joined));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final ConfiguredJoined<K, V, VT> joined) {
        return this.join(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, joined));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final ConfiguredJoined<K, V, VT> joined) {
        return this.leftJoin(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, joined));
    }

    @Override
    public <VT, VR> ImprovedKStream<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final ConfiguredJoined<K, V, VT> joined) {
        return this.leftJoin(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> joiner) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> joiner, final Named named) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner, named));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner, final Named named) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner, named));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner, final Named named) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner, named));
    }

    @Override
    public <GK, GV, RV> ImprovedKStream<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner, final Named named) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner, named));
    }

    @Override
    public <K1, V1> ImprovedKStream<K1, V1> transform(
            final TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transform(transformerSupplier, stateStoreNames));
    }

    @Override
    public <K1, V1> ImprovedKStream<K1, V1> transform(
            final TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transform(transformerSupplier, named, stateStoreNames));
    }

    @Override
    public <K1, V1> ImprovedKStream<K1, V1> flatTransform(
            final TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.flatTransform(transformerSupplier, stateStoreNames));
    }

    @Override
    public <K1, V1> ImprovedKStream<K1, V1> flatTransform(
            final TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
            final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.flatTransform(transformerSupplier, named, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> transformValues(
            final ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(valueTransformerSupplier, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> transformValues(
            final ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(valueTransformerSupplier, named, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(valueTransformerSupplier, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> transformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
            final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.transformValues(valueTransformerSupplier, named, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatTransformValues(
            final ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.flatTransformValues(valueTransformerSupplier, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatTransformValues(
            final ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(
                this.wrapped.flatTransformValues(valueTransformerSupplier, named, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatTransformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.flatTransformValues(valueTransformerSupplier, stateStoreNames));
    }

    @Override
    public <VR> ImprovedKStream<K, VR> flatTransformValues(
            final ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
            final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(
                this.wrapped.flatTransformValues(valueTransformerSupplier, named, stateStoreNames));
    }

    @Override
    public void process(
            final org.apache.kafka.streams.processor.ProcessorSupplier<? super K, ? super V> processorSupplier,
            final String... stateStoreNames) {
        this.wrapped.process(processorSupplier, stateStoreNames);
    }

    @Override
    public void process(
            final org.apache.kafka.streams.processor.ProcessorSupplier<? super K, ? super V> processorSupplier,
            final Named named, final String... stateStoreNames) {
        this.wrapped.process(processorSupplier, named, stateStoreNames);
    }

    @Override
    public <KOut, VOut> ImprovedKStream<KOut, VOut> process(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.process(processorSupplier, stateStoreNames));
    }

    @Override
    public <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(ErrorCapturingProcessor.captureErrors(processorSupplier),
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(
                ErrorCapturingProcessor.captureErrors(processorSupplier, errorFilter),
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> ImprovedKStream<KOut, VOut> process(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.process(processorSupplier, named, stateStoreNames));
    }

    @Override
    public <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(ErrorCapturingProcessor.captureErrors(processorSupplier), named,
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter,
            final Named named, final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(
                ErrorCapturingProcessor.captureErrors(processorSupplier, errorFilter),
                named, stateStoreNames);
    }

    @Override
    public <VOut> ImprovedKStream<K, VOut> processValues(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.processValues(processorSupplier, stateStoreNames));
    }

    @Override
    public <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(ErrorCapturingValueProcessor.captureErrors(processorSupplier),
                stateStoreNames);
    }

    @Override
    public <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter, final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(
                ErrorCapturingValueProcessor.captureErrors(processorSupplier, errorFilter),
                stateStoreNames
        );
    }

    @Override
    public <VOut> ImprovedKStream<K, VOut> processValues(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.processValues(processorSupplier, named, stateStoreNames));
    }

    @Override
    public <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(ErrorCapturingValueProcessor.captureErrors(processorSupplier),
                named,
                stateStoreNames);
    }

    @Override
    public <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter, final Named named,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(
                ErrorCapturingValueProcessor.captureErrors(processorSupplier, errorFilter), named, stateStoreNames);
    }

    private <KR, VR> KeyValueKErrorStream<K, V, KR, VR> mapCapturingErrorsInternal(
            final KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> mapper) {
        final ImprovedKStream<KR, ProcessedKeyValue<K, V, VR>> map = this.map(mapper);
        return new KeyValueKErrorStream<>(map);
    }

    private <KR, VR> KeyValueKErrorStream<K, V, KR, VR> mapCapturingErrorsInternal(
            final KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> mapper, final Named named) {
        final ImprovedKStream<KR, ProcessedKeyValue<K, V, VR>> map = this.map(mapper, named);
        return new KeyValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapper<V, ProcessedValue<V, VR>> mapper) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.mapValues(mapper);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapper<V, ProcessedValue<V, VR>> mapper,
            final Named named) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.mapValues(mapper, named);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, ProcessedValue<V, VR>> mapper) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.mapValues(mapper);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, ProcessedValue<V, VR>> mapper, final Named named) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.mapValues(mapper, named);
        return new ValueKErrorStream<>(map);
    }

    private <KR, VR> KeyValueKErrorStream<K, V, KR, VR> flatMapCapturingErrorsInternal(
            final KeyValueMapper<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> mapper) {
        final ImprovedKStream<KR, ProcessedKeyValue<K, V, VR>> map = this.flatMap(mapper);
        return new KeyValueKErrorStream<>(map);
    }

    private <KR, VR> KeyValueKErrorStream<K, V, KR, VR> flatMapCapturingErrorsInternal(
            final KeyValueMapper<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> mapper, final Named named) {
        final ImprovedKStream<KR, ProcessedKeyValue<K, V, VR>> map = this.flatMap(mapper, named);
        return new KeyValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapper<V, Iterable<ProcessedValue<V, VR>>> mapper) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapper<V, Iterable<ProcessedValue<V, VR>>> mapper, final Named named) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper, named);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> mapper) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper);
        return new ValueKErrorStream<>(map);
    }

    private <VR> ValueKErrorStream<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> mapper, final Named named) {
        final ImprovedKStream<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper, named);
        return new ValueKErrorStream<>(map);
    }

    private <KOut, VOut> KeyValueKErrorStream<K, V, KOut, VOut> processCapturingErrorsInternal(
            final ProcessorSupplier<K, V, KOut, ProcessedKeyValue<K, V, VOut>> processorSupplier,
            final String... stateStoreNames) {
        final ImprovedKStream<KOut, ProcessedKeyValue<K, V, VOut>> map =
                this.process(processorSupplier, stateStoreNames);
        return new KeyValueKErrorStream<>(map);
    }

    private <KOut, VOut> KeyValueKErrorStream<K, V, KOut, VOut> processCapturingErrorsInternal(
            final ProcessorSupplier<K, V, KOut, ProcessedKeyValue<K, V, VOut>> processorSupplier, final Named named,
            final String... stateStoreNames) {
        final ImprovedKStream<KOut, ProcessedKeyValue<K, V, VOut>> map =
                this.process(processorSupplier, named, stateStoreNames);
        return new KeyValueKErrorStream<>(map);
    }

    private <VOut> ValueKErrorStream<K, V, VOut> processValuesCapturingErrorsInternal(
            final FixedKeyProcessorSupplier<? super K, V, ProcessedValue<V, VOut>> processorSupplier,
            final String... stateStoreNames) {
        final ImprovedKStream<K, ProcessedValue<V, VOut>> map = this.processValues(processorSupplier, stateStoreNames);
        return new ValueKErrorStream<>(map);
    }

    private <VOut> ValueKErrorStream<K, V, VOut> processValuesCapturingErrorsInternal(
            final FixedKeyProcessorSupplier<? super K, V, ProcessedValue<V, VOut>> processorSupplier,
            final Named named, final String... stateStoreNames) {
        final ImprovedKStream<K, ProcessedValue<V, VOut>> map =
                this.processValues(processorSupplier, named, stateStoreNames);
        return new ValueKErrorStream<>(map);
    }
}
