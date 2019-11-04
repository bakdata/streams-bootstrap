package com.bakdata.common_kafka_streams.util;

import static org.jooq.lambda.Seq.seq;

import java.util.List;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Wrap a {@code ValueMapper} and capture thrown exceptions.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueMapper)
 * @see #captureErrors(ValueMapper, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingFlatValueMapper<V, VR> implements ValueMapper<V, Iterable<ProcessedValue<V, VR>>> {
    private final @NonNull ValueMapper<? super V, ? extends Iterable<VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueMapper} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema registry
     * timeout are forwarded and not captured.
     *
     * @see #captureErrors(ValueMapper, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <V, VR> ValueMapper<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueMapper<? super V, ? extends Iterable<VR>> mapper) {
        return captureErrors(mapper, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueMapper} and capture thrown exceptions.
     * <pre>{@code
     * final ValueMapper<V, Iterable<VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.flatMapValues(captureErrors(mapper));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapper} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final ValueMapper<? super V, ? extends Iterable<VR>> mapper, final Predicate<Exception> errorFilter) {
        return new ErrorCapturingFlatValueMapper<>(mapper, errorFilter);
    }

    @Override
    public Iterable<ProcessedValue<V, VR>> apply(final V value) {
        try {
            final Iterable<VR> newValues = this.wrapped.apply(value);
            return seq(newValues).map(SuccessValue::of);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return List.of(ErrorValue.of(value, e));
        }
    }

}
