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

import com.bakdata.kafka.ErrorCapturingFlatKeyValueMapper;
import com.bakdata.kafka.ErrorCapturingFlatValueMapper;
import com.bakdata.kafka.ErrorCapturingFlatValueMapperWithKey;
import com.bakdata.kafka.ErrorCapturingKeyValueMapper;
import com.bakdata.kafka.ErrorCapturingProcessor;
import com.bakdata.kafka.ErrorCapturingValueMapper;
import com.bakdata.kafka.ErrorCapturingValueMapperWithKey;
import com.bakdata.kafka.ErrorCapturingValueProcessor;
import com.bakdata.kafka.ProcessedKeyValue;
import com.bakdata.kafka.ProcessedValue;
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
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

@RequiredArgsConstructor
class KStreamXImpl<K, V> implements KStreamX<K, V> {

    @Getter(AccessLevel.PACKAGE)
    private final @NonNull KStream<K, V> wrapped;
    private final @NonNull StreamsContext context;

    @Override
    public KStreamX<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filter(predicate));
    }

    @Override
    public KStreamX<K, V> filter(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filter(predicate, named));
    }

    @Override
    public KStreamX<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        return this.context.wrap(this.wrapped.filterNot(predicate));
    }

    @Override
    public KStreamX<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final Named named) {
        return this.context.wrap(this.wrapped.filterNot(predicate, named));
    }

    @Override
    public <KR> KStreamX<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper) {
        return this.context.wrap(this.wrapped.selectKey(mapper));
    }

    @Override
    public <KR> KStreamX<KR, V> selectKey(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.selectKey(mapper, named));
    }

    @Override
    public <KR, VR> KStreamX<KR, VR> map(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.map(mapper));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <KR, VR> KStreamX<KR, VR> map(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.map(mapper, named));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final Named named) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> mapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapCapturingErrorsInternal(ErrorCapturingKeyValueMapper.captureErrors(mapper, errorFilter), named);
    }

    @Override
    public <VR> KStreamX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends VR> mapper) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> KStreamX<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(final ValueMapper<? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> KStreamX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return this.context.wrap(this.wrapped.mapValues(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapperWithKey.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.mapValuesCapturingErrorsInternal(
                ErrorCapturingValueMapperWithKey.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> KStreamX<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.mapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, final Named named) {
        return this.mapValuesCapturingErrorsInternal(ErrorCapturingValueMapperWithKey.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> mapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.mapValuesCapturingErrorsInternal(
                ErrorCapturingValueMapperWithKey.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <KR, VR> KStreamX<KR, VR> flatMap(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper) {
        return this.context.wrap(this.wrapped.flatMap(mapper));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <KR, VR> KStreamX<KR, VR> flatMap(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMap(mapper, named));
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final Named named) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <KR, VR> KErrorStreamX<K, V, KR, VR> flatMapCapturingErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ?
                    extends VR>>> mapper,
            final java.util.function.Predicate<Exception> errorFilter, final Named named) {
        return this.flatMapCapturingErrorsInternal(ErrorCapturingFlatKeyValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> KStreamX<K, VR> flatMapValues(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapper.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapper.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> KStreamX<K, VR> flatMapValues(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper, final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapper.captureErrors(mapper), named);
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter,
            final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapper.captureErrors(mapper, errorFilter),
                named);
    }

    @Override
    public <VR> KStreamX<K, VR> flatMapValues(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final java.util.function.Predicate<Exception> errorFilter) {
        return this.flatMapValuesCapturingErrorsInternal(
                ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper, errorFilter));
    }

    @Override
    public <VR> KStreamX<K, VR> flatMapValues(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.context.wrap(this.wrapped.flatMapValues(mapper, named));
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            final Named named) {
        return this.flatMapValuesCapturingErrorsInternal(ErrorCapturingFlatValueMapperWithKey.captureErrors(mapper),
                named);
    }

    @Override
    public <VR> KErrorStreamX<K, V, K, VR> flatMapValuesCapturingErrors(
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
    public KStreamX<K, V> peek(final ForeachAction<? super K, ? super V> action) {
        return this.context.wrap(this.wrapped.peek(action));
    }

    @Override
    public KStreamX<K, V> peek(final ForeachAction<? super K, ? super V> action, final Named named) {
        return this.context.wrap(this.wrapped.peek(action, named));
    }

    @Override
    public BranchedKStreamX<K, V> split() {
        return this.context.wrap(this.wrapped.split());
    }

    @Override
    public BranchedKStreamX<K, V> split(final Named named) {
        return this.context.wrap(this.wrapped.split(named));
    }

    @Override
    public KStreamX<K, V> merge(final KStream<K, V> stream) {
        final KStream<K, V> other = StreamsContext.maybeUnwrap(stream);
        return this.context.wrap(this.wrapped.merge(other));
    }

    @Override
    public KStreamX<K, V> merge(final KStream<K, V> stream, final Named named) {
        final KStream<K, V> other = StreamsContext.maybeUnwrap(stream);
        return this.context.wrap(this.wrapped.merge(other, named));
    }

    @Override
    public KStreamX<K, V> repartition() {
        return this.context.wrap(this.wrapped.repartition());
    }

    @Override
    public KStreamX<K, V> repartition(final Repartitioned<K, V> repartitioned) {
        return this.context.wrap(this.wrapped.repartition(repartitioned));
    }

    @Override
    public KStreamX<K, V> repartition(final RepartitionedX<K, V> repartitioned) {
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
    public void to(final String topic, final ProducedX<K, V> produced) {
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
    public void to(final TopicNameExtractor<K, V> topicExtractor, final ProducedX<K, V> produced) {
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
    public void toOutputTopic(final ProducedX<K, V> produced) {
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
    public void toOutputTopic(final String label, final ProducedX<K, V> produced) {
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
    public void toErrorTopic(final ProducedX<K, V> produced) {
        this.toErrorTopic(produced.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> toTable() {
        return this.context.wrap(this.wrapped.toTable());
    }

    @Override
    public KTableX<K, V> toTable(final Named named) {
        return this.context.wrap(this.wrapped.toTable(named));
    }

    @Override
    public KTableX<K, V> toTable(final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.toTable(materialized));
    }

    @Override
    public KTableX<K, V> toTable(final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.toTable(materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public KTableX<K, V> toTable(final Named named,
            final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.context.wrap(this.wrapped.toTable(named, materialized));
    }

    @Override
    public KTableX<K, V> toTable(final Named named,
            final MaterializedX<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        return this.toTable(named, materialized.configure(this.context.getConfigurator()));
    }

    @Override
    public <KR> KGroupedStreamX<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector) {
        return this.context.wrap(this.wrapped.groupBy(keySelector));
    }

    @Override
    public <KR> KGroupedStreamX<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector,
            final Grouped<KR, V> grouped) {
        return this.context.wrap(this.wrapped.groupBy(keySelector, grouped));
    }

    @Override
    public <KR> KGroupedStreamX<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> keySelector,
            final GroupedX<KR, V> grouped) {
        return this.groupBy(keySelector, grouped.configure(this.context.getConfigurator()));
    }

    @Override
    public KGroupedStreamX<K, V> groupByKey() {
        return this.context.wrap(this.wrapped.groupByKey());
    }

    @Override
    public KGroupedStreamX<K, V> groupByKey(final Grouped<K, V> grouped) {
        return this.context.wrap(this.wrapped.groupByKey(grouped));
    }

    @Override
    public KGroupedStreamX<K, V> groupByKey(final GroupedX<K, V> grouped) {
        return this.groupByKey(grouped.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.join(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.join(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> join(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.join(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.leftJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> leftJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.leftJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner,
            final JoinWindows windows) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.outerJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoined<K, V, VO> streamJoined) {
        final KStream<K, VO> other = StreamsContext.maybeUnwrap(otherStream);
        return this.context.wrap(this.wrapped.outerJoin(other, joiner, windows, streamJoined));
    }

    @Override
    public <VO, VR> KStreamX<K, VR> outerJoin(final KStream<K, VO> otherStream,
            final ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, final JoinWindows windows,
            final StreamJoinedX<K, V, VO> streamJoined) {
        return this.outerJoin(otherStream, joiner, windows, streamJoined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner, joined));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final JoinedX<K, V, VT> joined) {
        return this.join(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.join(other, joiner, joined));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> join(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final JoinedX<K, V, VT> joined) {
        return this.join(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, joined));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoiner<? super V, ? super VT, ? extends VR> joiner, final JoinedX<K, V, VT> joined) {
        return this.leftJoin(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final Joined<K, V, VT> joined) {
        final KTable<K, VT> other = StreamsContext.maybeUnwrap(table);
        return this.context.wrap(this.wrapped.leftJoin(other, joiner, joined));
    }

    @Override
    public <VT, VR> KStreamX<K, VR> leftJoin(final KTable<K, VT> table,
            final ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            final JoinedX<K, V, VT> joined) {
        return this.leftJoin(table, joiner, joined.configure(this.context.getConfigurator()));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> joiner) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> joiner, final Named named) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner, named));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> join(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner, final Named named) {
        return this.context.wrap(this.wrapped.join(globalTable, keySelector, joiner, named));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner, final Named named) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner, named));
    }

    @Override
    public <GK, GV, RV> KStreamX<K, RV> leftJoin(final GlobalKTable<GK, GV> globalTable,
            final KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            final ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner, final Named named) {
        return this.context.wrap(this.wrapped.leftJoin(globalTable, keySelector, valueJoiner, named));
    }

    @Override
    public <KOut, VOut> KStreamX<KOut, VOut> process(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.process(processorSupplier, stateStoreNames));
    }

    @Override
    public <KOut, VOut> KErrorStreamX<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(ErrorCapturingProcessor.captureErrors(processorSupplier),
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> KErrorStreamX<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(
                ErrorCapturingProcessor.captureErrors(processorSupplier, errorFilter),
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> KStreamX<KOut, VOut> process(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.process(processorSupplier, named, stateStoreNames));
    }

    @Override
    public <KOut, VOut> KErrorStreamX<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(ErrorCapturingProcessor.captureErrors(processorSupplier), named,
                stateStoreNames);
    }

    @Override
    public <KOut, VOut> KErrorStreamX<K, V, KOut, VOut> processCapturingErrors(
            final ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter,
            final Named named, final String... stateStoreNames) {
        return this.processCapturingErrorsInternal(
                ErrorCapturingProcessor.captureErrors(processorSupplier, errorFilter),
                named, stateStoreNames);
    }

    @Override
    public <VOut> KStreamX<K, VOut> processValues(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.processValues(processorSupplier, stateStoreNames));
    }

    @Override
    public <VOut> KErrorStreamX<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(ErrorCapturingValueProcessor.captureErrors(processorSupplier),
                stateStoreNames);
    }

    @Override
    public <VOut> KErrorStreamX<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter, final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(
                ErrorCapturingValueProcessor.captureErrors(processorSupplier, errorFilter),
                stateStoreNames
        );
    }

    @Override
    public <VOut> KStreamX<K, VOut> processValues(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.context.wrap(this.wrapped.processValues(processorSupplier, named, stateStoreNames));
    }

    @Override
    public <VOut> KErrorStreamX<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier, final Named named,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(ErrorCapturingValueProcessor.captureErrors(processorSupplier),
                named,
                stateStoreNames);
    }

    @Override
    public <VOut> KErrorStreamX<K, V, K, VOut> processValuesCapturingErrors(
            final FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            final java.util.function.Predicate<Exception> errorFilter, final Named named,
            final String... stateStoreNames) {
        return this.processValuesCapturingErrorsInternal(
                ErrorCapturingValueProcessor.captureErrors(processorSupplier, errorFilter), named, stateStoreNames);
    }

    private <KR, VR> KeyValueKErrorStreamX<K, V, KR, VR> mapCapturingErrorsInternal(
            final KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> mapper) {
        final KStreamX<KR, ProcessedKeyValue<K, V, VR>> map = this.map(mapper);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <KR, VR> KeyValueKErrorStreamX<K, V, KR, VR> mapCapturingErrorsInternal(
            final KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> mapper, final Named named) {
        final KStreamX<KR, ProcessedKeyValue<K, V, VR>> map = this.map(mapper, named);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapper<V, ProcessedValue<V, VR>> mapper) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.mapValues(mapper);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapper<V, ProcessedValue<V, VR>> mapper,
            final Named named) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.mapValues(mapper, named);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, ProcessedValue<V, VR>> mapper) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.mapValues(mapper);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> mapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, ProcessedValue<V, VR>> mapper, final Named named) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.mapValues(mapper, named);
        return new ValueKErrorStreamX<>(map);
    }

    private <KR, VR> KeyValueKErrorStreamX<K, V, KR, VR> flatMapCapturingErrorsInternal(
            final KeyValueMapper<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> mapper) {
        final KStreamX<KR, ProcessedKeyValue<K, V, VR>> map = this.flatMap(mapper);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <KR, VR> KeyValueKErrorStreamX<K, V, KR, VR> flatMapCapturingErrorsInternal(
            final KeyValueMapper<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> mapper, final Named named) {
        final KStreamX<KR, ProcessedKeyValue<K, V, VR>> map = this.flatMap(mapper, named);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapper<V, Iterable<ProcessedValue<V, VR>>> mapper) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapper<V, Iterable<ProcessedValue<V, VR>>> mapper, final Named named) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper, named);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> mapper) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper);
        return new ValueKErrorStreamX<>(map);
    }

    private <VR> ValueKErrorStreamX<K, V, VR> flatMapValuesCapturingErrorsInternal(
            final ValueMapperWithKey<K, V, Iterable<ProcessedValue<V, VR>>> mapper, final Named named) {
        final KStreamX<K, ProcessedValue<V, VR>> map = this.flatMapValues(mapper, named);
        return new ValueKErrorStreamX<>(map);
    }

    private <KOut, VOut> KeyValueKErrorStreamX<K, V, KOut, VOut> processCapturingErrorsInternal(
            final ProcessorSupplier<K, V, KOut, ProcessedKeyValue<K, V, VOut>> processorSupplier,
            final String... stateStoreNames) {
        final KStreamX<KOut, ProcessedKeyValue<K, V, VOut>> map =
                this.process(processorSupplier, stateStoreNames);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <KOut, VOut> KeyValueKErrorStreamX<K, V, KOut, VOut> processCapturingErrorsInternal(
            final ProcessorSupplier<K, V, KOut, ProcessedKeyValue<K, V, VOut>> processorSupplier, final Named named,
            final String... stateStoreNames) {
        final KStreamX<KOut, ProcessedKeyValue<K, V, VOut>> map =
                this.process(processorSupplier, named, stateStoreNames);
        return new KeyValueKErrorStreamX<>(map);
    }

    private <VOut> ValueKErrorStreamX<K, V, VOut> processValuesCapturingErrorsInternal(
            final FixedKeyProcessorSupplier<? super K, V, ProcessedValue<V, VOut>> processorSupplier,
            final String... stateStoreNames) {
        final KStreamX<K, ProcessedValue<V, VOut>> map = this.processValues(processorSupplier, stateStoreNames);
        return new ValueKErrorStreamX<>(map);
    }

    private <VOut> ValueKErrorStreamX<K, V, VOut> processValuesCapturingErrorsInternal(
            final FixedKeyProcessorSupplier<? super K, V, ProcessedValue<V, VOut>> processorSupplier,
            final Named named, final String... stateStoreNames) {
        final KStreamX<K, ProcessedValue<V, VOut>> map =
                this.processValues(processorSupplier, named, stateStoreNames);
        return new ValueKErrorStreamX<>(map);
    }
}
