package com.bakdata.common_kafka_streams.util;

import static java.util.Collections.emptyList;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

@Slf4j
@RequiredArgsConstructor
public class ErrorLoggingFlatKeyValueMapper<K, V, KR, VR> implements KeyValueMapper<K, V, Iterable<KeyValue<KR, VR>>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> wrapped;

    public static <K, V, KR, VR> KeyValueMapper<K, V, Iterable<KeyValue<KR, VR>>> logErrors(
            final KeyValueMapper<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> mapper) {
        return new ErrorLoggingFlatKeyValueMapper<>(mapper);
    }

    @Override
    public Iterable<KeyValue<KR, VR>> apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            if (ErrorUtil.shouldForwardError(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return emptyList();
        }
    }
}
