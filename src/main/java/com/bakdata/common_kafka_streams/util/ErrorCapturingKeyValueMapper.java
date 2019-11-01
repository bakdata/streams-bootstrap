package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorCapturingKeyValueMapper<K, V, KR, VR>
        implements KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends KeyValue<KR, VR>> wrapped;

    /**
     * Wrap a {@code KeyValueMapper} and capture thrown exceptions.
     * <pre>{@code
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * Recoverable Kafka exceptions such as a schema registry timeout are forwarded and not captured. See {@link
     * ErrorUtil#shouldForwardError(Exception)}
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code KeyValueMapper}
     */
    public static <K, V, KR, VR> KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final KeyValueMapper<? super K, ? super V, ? extends KeyValue<KR, VR>> mapper) {
        return new ErrorCapturingKeyValueMapper<>(mapper);
    }

    @Override
    public KeyValue<KR, ProcessedKeyValue<K, V, VR>> apply(final K key, final V value) {
        try {
            final KeyValue<KR, VR> newKeyValue = this.wrapped.apply(key, value);
            final ProcessedKeyValue<K, V, VR> recordWithOldKey = SuccessKeyValue.of(newKeyValue.value);
            return KeyValue.pair(newKeyValue.key, recordWithOldKey);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(key, value, e);
            // new key is only relevant if no error occurs
            return KeyValue.pair(null, errorWithOldKey);
        }
    }
}
