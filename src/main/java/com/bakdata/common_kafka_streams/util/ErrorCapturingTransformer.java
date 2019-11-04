package com.bakdata.common_kafka_streams.util;

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

/**
 * Wrap a {@code Transformer} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #captureErrors(Transformer)
 * @see #captureErrors(Transformer, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingTransformer<K, V, KR, VR>
        implements Transformer<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> {
    private final @NonNull Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code Transformer} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema registry
     * timeout are forwarded and not captured.
     *
     * @see #captureErrors(Transformer, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, KR, VR> Transformer<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> transformer) {
        return captureErrors(transformer, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code Transformer} and capture thrown exceptions.
     * <pre>{@code
     * final TransformerSupplier<K, V, KeyValue<KR, VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.transform(() -> captureErrors(transformer.get()));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Transformer}
     */
    public static <K, V, KR, VR> Transformer<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final Transformer<? super K, ? super V, ? extends KeyValue<KR, VR>> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingTransformer<>(transformer, errorFilter);
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(new ErrorCapturingProcessorContext(context));
    }

    @Override
    public KeyValue<KR, ProcessedKeyValue<K, V, VR>> transform(final K key, final V value) {
        try {
            final KeyValue<KR, VR> newKeyValue = this.wrapped.transform(key, value);
            if (newKeyValue == null) {
                return null;
            }
            final ProcessedKeyValue<K, V, VR> recordWithOldKey = SuccessKeyValue.of(newKeyValue.value);
            return KeyValue.pair(newKeyValue.key, recordWithOldKey);
        } catch (final Exception e) {
            if (errorFilter.test(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(key, value, e);
            // new key is only relevant if no error occurs
            return KeyValue.pair(null, errorWithOldKey);
        }
    }

    private static final class ErrorCapturingProcessorContext extends DecoratorProcessorContext {
        private ErrorCapturingProcessorContext(final @NonNull ProcessorContext wrapped) {
            super(wrapped);
        }

        private static <K, V, VR> ProcessedKeyValue<K, V, VR> getValue(final VR value) {
            return SuccessKeyValue.of(value);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, to);
        }

        @Override
        public <K, V> void forward(final K key, final V value) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, childIndex);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {
            final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
            super.forward(key, recordWithOldKey, childName);
        }
    }

}
