package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(ValueMapperWithKey)
 * @see #logErrors(ValueMapperWithKey, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingValueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @see #logErrors(ValueMapperWithKey, Predicate)
     * @see ErrorUtil#shouldForwardError(Exception)
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, Iterable<VR>> logErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        return logErrors(mapper, ErrorUtil::shouldForwardError);
    }

    /**
     * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapperWithKey<K, V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.mapValues(logErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, Iterable<VR>> logErrors(
            final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
            final Predicate<Exception> errorFilter) {
        return new ErrorLoggingValueMapperWithKey<>(mapper, errorFilter);
    }

    @Override
    public Iterable<VR> apply(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.apply(key, value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return emptyList();
        }
    }
}
