package com.bakdata.common_kafka_streams.util;

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

/**
 * Wrap a {@code Transformer} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of transformation result
 * @see #logErrors(Transformer)
 * @see #logErrors(Transformer, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingTransformer<K, V, R> implements Transformer<K, V, R> {
    private final @NonNull Transformer<? super K, ? super V, ? extends R> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code Transformer} and log thrown exceptions with input key and value. Recoverable Kafka exceptions such
     * as a schema registry timeout are forwarded and not captured.
     *
     * @see #logErrors(Transformer, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, R> Transformer<K, V, R> logErrors(
            final Transformer<? super K, ? super V, ? extends R> transformer) {
        return logErrors(transformer, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code Transformer} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final TransformerSupplier<K, V, KeyValue<KR, VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(() -> logErrors(transformer.get()));
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code Transformer}
     */
    public static <K, V, R> Transformer<K, V, R> logErrors(
            final Transformer<? super K, ? super V, ? extends R> transformer, final Predicate<Exception> errorFilter) {
        return new ErrorLoggingTransformer<>(transformer, errorFilter);
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
    public R transform(final K key, final V value) {
        try {
            return this.wrapped.transform(key, value);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return null;
        }
    }

    private static final class ErrorCapturingProcessorContext extends DecoratorProcessorContext {
        private ErrorCapturingProcessorContext(final @NonNull ProcessorContext wrapped) {
            super(wrapped);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final To to) {
            super.forward(key, value, to);
        }

        @Override
        public <K, V> void forward(final K key, final V value) {
            super.forward(key, value);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final int childIndex) {
            super.forward(key, value, childIndex);
        }

        @Override
        public <K, V> void forward(final K key, final V value, final String childName) {
            super.forward(key, value, childName);
        }
    }

}
