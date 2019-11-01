package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import java.util.Collections;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingValueTransformerWithKey<K, V, VR> implements ValueTransformerWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerWithKey<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> logErrors(transformer));
     * }
     * </pre>
     *
     * Recoverable Kafka exceptions such as a schema registry timeout are forwarded and not captured. See {@link
     * ErrorUtil#shouldForwardError(Exception)}
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer) {
        return new ErrorLoggingValueTransformerWithKey<>(transformer);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public Iterable<VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            // allow null values
            return Collections.singletonList(newValue);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return emptyList();
        }
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

}
