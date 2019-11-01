package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerWithKey<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.transformValues(() -> captureErrors(transformer));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * Recoverable Kafka exceptions such as a schema registry timeout are forwarded and not captured. See {@link
     * ErrorUtil#shouldForwardError(Exception)}
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer) {
        return new ErrorCapturingValueTransformerWithKey<>(transformer);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public ProcessedValue<V, VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

}
