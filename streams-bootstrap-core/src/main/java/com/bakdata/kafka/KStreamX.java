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
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Extends the {@code KStream} interface by adding methods to simplify Serde configuration, error handling, and topic
 * access
 * @param <K> type of keys
 * @param <V> type of values
 */
public interface KStreamX<K, V> extends KStream<K, V> {

    @Override
    KStreamX<K, V> filter(Predicate<? super K, ? super V> predicate);

    @Override
    KStreamX<K, V> filter(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    KStreamX<K, V> filterNot(Predicate<? super K, ? super V> predicate);

    @Override
    KStreamX<K, V> filterNot(Predicate<? super K, ? super V> predicate, Named named);

    @Override
    <KR> KStreamX<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);

    @Override
    <KR> KStreamX<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper, Named named);

    @Override
    <KR, VR> KStreamX<KR, VR> map(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper);

    /**
     * Transform each record of the input stream into a new record in the output stream. Errors in the mapper are
     * captured
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #map(KeyValueMapper)
     * @see ErrorCapturingKeyValueMapper#captureErrors(KeyValueMapper)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper);

    /**
     * Transform each record of the input stream into a new record in the output stream. Errors in the mapper are
     * captured
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #map(KeyValueMapper)
     * @see ErrorCapturingKeyValueMapper#captureErrors(KeyValueMapper, java.util.function.Predicate)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <KR, VR> KStreamX<KR, VR> map(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named);

    /**
     * Transform each record of the input stream into a new record in the output stream. Errors in the mapper are
     * captured
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #map(KeyValueMapper, Named)
     * @see ErrorCapturingKeyValueMapper#captureErrors(KeyValueMapper)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper, Named named);

    /**
     * Transform each record of the input stream into a new record in the output stream. Errors in the mapper are
     * captured
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #map(KeyValueMapper, Named)
     * @see ErrorCapturingKeyValueMapper#captureErrors(KeyValueMapper, java.util.function.Predicate)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> mapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> KStreamX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapper)
     * @see ErrorCapturingValueMapper#captureErrors(ValueMapper)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapper)
     * @see ErrorCapturingValueMapper#captureErrors(ValueMapper, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> KStreamX<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper, Named named);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapper, Named)
     * @see ErrorCapturingValueMapper#captureErrors(ValueMapper)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper, Named named);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapper, Named)
     * @see ErrorCapturingValueMapper#captureErrors(ValueMapper, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(ValueMapper<? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> KStreamX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes a new output value
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapperWithKey)
     * @see ErrorCapturingValueMapperWithKey#captureErrors(ValueMapperWithKey)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes a new output value
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapperWithKey)
     * @see ErrorCapturingValueMapperWithKey#captureErrors(ValueMapperWithKey, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> KStreamX<K, VR> mapValues(ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes a new output value
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapperWithKey, Named)
     * @see ErrorCapturingValueMapperWithKey#captureErrors(ValueMapperWithKey)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper, Named named);

    /**
     * Transform the value of each input record into a new value of the output record. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes a new output value
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VR> the value type of the result stream
     * @see #mapValues(ValueMapperWithKey, Named)
     * @see ErrorCapturingValueMapperWithKey#captureErrors(ValueMapperWithKey, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> mapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <KR, VR> KStreamX<KR, VR> flatMap(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    /**
     * Transform each record of the input stream into zero or more records in the output stream. Errors in the mapper
     * are captured
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @return a {@link KErrorStream} that contains more or less records with new key and value as well as captured
     * errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #flatMap(KeyValueMapper)
     * @see ErrorCapturingFlatKeyValueMapper#captureErrors(KeyValueMapper)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    /**
     * Transform each record of the input stream into zero or more records in the output stream. Errors in the mapper
     * are captured
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains more or less records with new key and value as well as captured
     * errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #flatMap(KeyValueMapper)
     * @see ErrorCapturingFlatKeyValueMapper#captureErrors(KeyValueMapper, java.util.function.Predicate)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <KR, VR> KStreamX<KR, VR> flatMap(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            Named named);

    /**
     * Transform each record of the input stream into zero or more records in the output stream. Errors in the mapper
     * are captured
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with new key and value as well as captured
     * errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #flatMap(KeyValueMapper, Named)
     * @see ErrorCapturingFlatKeyValueMapper#captureErrors(KeyValueMapper)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            Named named);

    /**
     * Transform each record of the input stream into zero or more records in the output stream. Errors in the mapper
     * are captured
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with new key and value as well as captured
     * errors
     * @param <KR> the key type of the result stream
     * @param <VR> the value type of the result stream
     * @see #flatMap(KeyValueMapper, Named)
     * @see ErrorCapturingFlatKeyValueMapper#captureErrors(KeyValueMapper, java.util.function.Predicate)
     */
    <KR, VR> KErrorStream<K, V, KR, VR> flatMapCapturingErrors(
            KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> KStreamX<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes the new output values
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapper)
     * @see ErrorCapturingFlatValueMapper#captureErrors(ValueMapper)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes the new output values
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapper)
     * @see ErrorCapturingFlatValueMapper#captureErrors(ValueMapper, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> KStreamX<K, VR> flatMapValues(ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes the new output values
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapper, Named)
     * @see ErrorCapturingFlatValueMapper#captureErrors(ValueMapper)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapper} that computes the new output values
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapper, Named)
     * @see ErrorCapturingFlatValueMapper#captureErrors(ValueMapper, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapper<? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    <VR> KStreamX<K, VR> flatMapValues(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes the new output values
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapperWithKey)
     * @see ErrorCapturingFlatValueMapperWithKey#captureErrors(ValueMapperWithKey)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes the new output values
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapperWithKey)
     * @see ErrorCapturingFlatValueMapperWithKey#captureErrors(ValueMapperWithKey, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter);

    @Override
    <VR> KStreamX<K, VR> flatMapValues(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            Named named);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes the new output values
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapperWithKey, Named)
     * @see ErrorCapturingFlatValueMapperWithKey#captureErrors(ValueMapperWithKey)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper, Named named);

    /**
     * Create a new {@link KStream} by transforming the value of each record in this stream into zero or more values
     * with
     * the same key in the new stream. Errors in the mapper are captured
     * @param mapper a {@link ValueMapperWithKey} that computes the new output values
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @return a {@link KErrorStream} that contains more or less records with unmodified keys and new values as well
     * as captured errors
     * @param <VR> the value type of the result stream
     * @see #flatMapValues(ValueMapperWithKey, Named)
     * @see ErrorCapturingFlatValueMapperWithKey#captureErrors(ValueMapperWithKey, java.util.function.Predicate)
     */
    <VR> KErrorStream<K, V, K, VR> flatMapValuesCapturingErrors(
            ValueMapperWithKey<? super K, ? super V, ? extends Iterable<? extends VR>> mapper,
            java.util.function.Predicate<Exception> errorFilter, Named named);

    @Override
    KStreamX<K, V> peek(ForeachAction<? super K, ? super V> action);

    @Override
    KStreamX<K, V> peek(ForeachAction<? super K, ? super V> action, Named named);

    @Override
    KStreamX<K, V>[] branch(Named named, Predicate<? super K, ? super V>... predicates);

    @Override
    KStreamX<K, V>[] branch(Predicate<? super K, ? super V>... predicates);

    @Override
    BranchedKStreamX<K, V> split();

    @Override
    BranchedKStreamX<K, V> split(Named named);

    @Override
    KStreamX<K, V> merge(KStream<K, V> stream);

    @Override
    KStreamX<K, V> merge(KStream<K, V> stream, Named named);

    @Override
    KStreamX<K, V> through(String topic);

    @Override
    KStreamX<K, V> through(String topic, Produced<K, V> produced);

    @Override
    KStreamX<K, V> repartition();

    @Override
    KStreamX<K, V> repartition(Repartitioned<K, V> repartitioned);

    /**
     * @see #repartition(Repartitioned)
     */
    KStreamX<K, V> repartition(AutoRepartitioned<K, V> repartitioned);

    /**
     * @see #to(String, Produced)
     */
    void to(String topic, AutoProduced<K, V> produced);

    /**
     * @see #to(TopicNameExtractor, Produced)
     */
    void to(TopicNameExtractor<K, V> topicExtractor, AutoProduced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic()}
     * @see #to(String)
     */
    void toOutputTopic();

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic()}
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toOutputTopic(Produced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic()}
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toOutputTopic(AutoProduced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic(String)}
     * @param label label of output topic
     * @see #to(String)
     */
    void toOutputTopic(String label);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic(String)}
     * @param label label of output topic
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toOutputTopic(String label, Produced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getOutputTopic(String)}
     * @param label label of output topic
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toOutputTopic(String label, AutoProduced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getErrorTopic()}
     * @see #to(String)
     */
    void toErrorTopic();

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getErrorTopic()}
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toErrorTopic(Produced<K, V> produced);

    /**
     * Materialize {@link KStream} to {@link StreamsTopicConfig#getErrorTopic()}
     * @param produced define optional parameters for materializing the stream
     * @see #to(String, Produced)
     */
    void toErrorTopic(AutoProduced<K, V> produced);

    @Override
    KTableX<K, V> toTable();

    @Override
    KTableX<K, V> toTable(Named named);

    @Override
    KTableX<K, V> toTable(Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #toTable(Materialized)
     */
    KTableX<K, V> toTable(AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    KTableX<K, V> toTable(Named named, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * @see #toTable(Named, Materialized)
     */
    KTableX<K, V> toTable(Named named, AutoMaterialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <KR> KGroupedStreamX<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> keySelector);

    @Override
    <KR> KGroupedStreamX<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> keySelector,
            Grouped<KR, V> grouped);

    /**
     * @see #groupBy(KeyValueMapper, Grouped)
     */
    <KR> KGroupedStreamX<KR, V> groupBy(KeyValueMapper<? super K, ? super V, KR> keySelector,
            AutoGrouped<KR, V> grouped);

    @Override
    KGroupedStreamX<K, V> groupByKey();

    @Override
    KGroupedStreamX<K, V> groupByKey(Grouped<K, V> grouped);

    /**
     * @see #groupByKey(Grouped)
     */
    KGroupedStreamX<K, V> groupByKey(AutoGrouped<K, V> grouped);

    @Override
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            JoinWindows windows, StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #join(KStream, ValueJoiner, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
            JoinWindows windows, AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #join(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> join(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #leftJoin(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> leftJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows);

    @Override
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            StreamJoined<K, V, VO> streamJoined);

    /**
     * @see #outerJoin(KStream, ValueJoinerWithKey, JoinWindows, StreamJoined)
     */
    <VO, VR> KStreamX<K, VR> outerJoin(KStream<K, VO> otherStream,
            ValueJoinerWithKey<? super K, ? super V, ? super VO, ? extends VR> joiner, JoinWindows windows,
            AutoStreamJoined<K, V, VO> streamJoined);

    @Override
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            Joined<K, V, VT> joined);

    /**
     * @see #join(KTable, ValueJoiner, Joined)
     */
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table, ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            AutoJoined<K, V, VT> joined);

    @Override
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined);

    /**
     * @see #join(KTable, ValueJoinerWithKey, Joined)
     */
    <VT, VR> KStreamX<K, VR> join(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            AutoJoined<K, V, VT> joined);

    @Override
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner);

    @Override
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            Joined<K, V, VT> joined);

    /**
     * @see #leftJoin(KTable, ValueJoiner, Joined)
     */
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
            AutoJoined<K, V, VT> joined);

    @Override
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner, Joined<K, V, VT> joined);

    /**
     * @see #leftJoin(KTable, ValueJoinerWithKey, Joined)
     */
    <VT, VR> KStreamX<K, VR> leftJoin(KTable<K, VT> table,
            ValueJoinerWithKey<? super K, ? super V, ? super VT, ? extends VR> joiner,
            AutoJoined<K, V, VT> joined);

    @Override
    <GK, GV, RV> KStreamX<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> joiner);

    @Override
    <GK, GV, RV> KStreamX<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner);

    @Override
    <GK, GV, RV> KStreamX<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> joiner, Named named);

    @Override
    <GK, GV, RV> KStreamX<K, RV> join(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> joiner, Named named);

    @Override
    <GK, GV, RV> KStreamX<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner);

    @Override
    <GK, GV, RV> KStreamX<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner);

    @Override
    <GK, GV, RV> KStreamX<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner, Named named);

    @Override
    <GK, GV, RV> KStreamX<K, RV> leftJoin(GlobalKTable<GK, GV> globalTable,
            KeyValueMapper<? super K, ? super V, ? extends GK> keySelector,
            ValueJoinerWithKey<? super K, ? super V, ? super GV, ? extends RV> valueJoiner, Named named);

    @Override
    <K1, V1> KStreamX<K1, V1> transform(
            TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
            String... stateStoreNames);

    @Override
    <K1, V1> KStreamX<K1, V1> transform(
            TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <K1, V1> KStreamX<K1, V1> flatTransform(
            TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier,
            String... stateStoreNames);

    @Override
    <K1, V1> KStreamX<K1, V1> flatTransform(
            TransformerSupplier<? super K, ? super V, Iterable<KeyValue<K1, V1>>> transformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> transformValues(
            ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> transformValues(
            ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> transformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> valueTransformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> flatTransformValues(
            ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> flatTransformValues(
            ValueTransformerSupplier<? super V, Iterable<VR>> valueTransformerSupplier,
            Named named, String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> flatTransformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier,
            String... stateStoreNames);

    @Override
    <VR> KStreamX<K, VR> flatTransformValues(
            ValueTransformerWithKeySupplier<? super K, ? super V, Iterable<VR>> valueTransformerSupplier, Named named,
            String... stateStoreNames);

    @Override
    <KOut, VOut> KStreamX<KOut, VOut> process(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.Processor}. Errors in the mapper are captured
     * @param processorSupplier an instance of {@link ProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.Processor}
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     * @see #process(ProcessorSupplier, String...)
     * @see ErrorCapturingProcessor#captureErrors(ProcessorSupplier)
     */
    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.Processor}. Errors in the mapper are captured
     * @param processorSupplier an instance of {@link ProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.Processor}
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     * @see #process(ProcessorSupplier, String...)
     * @see ErrorCapturingProcessor#captureErrors(ProcessorSupplier, java.util.function.Predicate)
     */
    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            String... stateStoreNames);

    @Override
    <KOut, VOut> KStreamX<KOut, VOut> process(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.Processor}. Errors in the mapper are captured
     * @param processorSupplier an instance of {@link ProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.Processor}
     * @param named a {@link Named} config used to name the processor in the topology
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     * @see #process(ProcessorSupplier, Named, String...)
     * @see ErrorCapturingProcessor#captureErrors(ProcessorSupplier)
     */
    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.Processor}. Errors in the mapper are captured
     * @param processorSupplier an instance of {@link ProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.Processor}
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with new key and value as well as captured errors
     * @param <KOut> the key type of the result stream
     * @param <VOut> the value type of the result stream
     * @see #process(ProcessorSupplier, Named, String...)
     * @see ErrorCapturingProcessor#captureErrors(ProcessorSupplier, java.util.function.Predicate)
     */
    <KOut, VOut> KErrorStream<K, V, KOut, VOut> processCapturingErrors(
            ProcessorSupplier<? super K, ? super V, KOut, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            Named named, String... stateStoreNames);

    @Override
    <VOut> KStreamX<K, VOut> processValues(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}. Errors in the
     * mapper are captured
     * @param processorSupplier an instance of {@link FixedKeyProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VOut> the value type of the result stream
     * @see #processValues(FixedKeyProcessorSupplier, String...)
     * @see ErrorCapturingValueProcessor#captureErrors(FixedKeyProcessorSupplier)
     */
    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}. Errors in the
     * mapper are captured
     * @param processorSupplier an instance of {@link FixedKeyProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VOut> the value type of the result stream
     * @see #processValues(FixedKeyProcessorSupplier, String...)
     * @see ErrorCapturingValueProcessor#captureErrors(FixedKeyProcessorSupplier, java.util.function.Predicate)
     */
    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            String... stateStoreNames);

    @Override
    <VOut> KStreamX<K, VOut> processValues(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}. Errors in the
     * mapper are captured
     * @param processorSupplier an instance of {@link FixedKeyProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}
     * @param named a {@link Named} config used to name the processor in the topology
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VOut> the value type of the result stream
     * @see #processValues(FixedKeyProcessorSupplier, Named, String...)
     * @see ErrorCapturingValueProcessor#captureErrors(FixedKeyProcessorSupplier)
     */
    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            Named named, String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}. Errors in the
     * mapper are captured
     * @param processorSupplier an instance of {@link FixedKeyProcessorSupplier} that generates a newly constructed
     * {@link org.apache.kafka.streams.processor.api.FixedKeyProcessor}
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param named a {@link Named} config used to name the processor in the topology
     * @param stateStoreNames the names of the state store used by the processor
     * @return a {@link KErrorStream} that contains records with unmodified key and new values as well as captured
     * errors
     * @param <VOut> the value type of the result stream
     * @see #processValues(FixedKeyProcessorSupplier, Named, String...)
     * @see ErrorCapturingValueProcessor#captureErrors(FixedKeyProcessorSupplier, java.util.function.Predicate)
     */
    <VOut> KErrorStream<K, V, K, VOut> processValuesCapturingErrors(
            FixedKeyProcessorSupplier<? super K, ? super V, VOut> processorSupplier,
            java.util.function.Predicate<Exception> errorFilter,
            Named named, String... stateStoreNames);
}
