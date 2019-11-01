package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingValueTransformer<V, VR> implements ValueTransformer<V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformer<? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueTransformer} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformer<V, VR> transformer = ...;
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
     * @param transformer {@code ValueTransformer} whose exceptions should be captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> ValueTransformer<V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformer<? super V, ? extends VR> transformer) {
        return new ErrorCapturingValueTransformer<>(transformer);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public ProcessedValue<V, VR> transform(final V value) {
        try {
            final VR newValue = this.wrapped.transform(value);
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
