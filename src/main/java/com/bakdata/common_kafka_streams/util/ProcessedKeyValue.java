package com.bakdata.common_kafka_streams.util;

import org.apache.kafka.streams.KeyValue;

/**
 * A processed key-value is created upon capturing errors in a streams topology. It can either contain a successfully
 * processed value or the old key along with a {@link ProcessingError} describing the input value and the {@link
 * Exception} that has been thrown.
 *
 * @param <K> the type of the old key before applying the error capturer
 * @param <V> the type of the old value before applying the error capturer
 * @param <VR> the type of the new value after applying the error capturer
 */
public interface ProcessedKeyValue<K, V, VR> {

    /**
     * <p>This method serves as a utility method to extract errors from a previous key-value mapper step. It can be
     * used as a lambda method reference and simply delegates to {@link #getErrors()}. The new key is not relevant and
     * thus omitted.</p>
     *
     * Usage example:<br>
     * <code>
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> input = ...;<br> final KStream<K, ProcessingError<V, VR>> errors =
     * input.flatMap(ProcessedKeyValue::getErrors);
     * </code>
     *
     * @param newKey the new key of a processed key-value pair. As this method extracts errors, the new key is not
     * relevant and omitted. It is only used as a parameter to use a method reference lambda when creating a streams
     * topology.
     * @param recordWithOldKey a processed key-value pair containing a successfully extracted record or a {@link
     * ProcessingError}
     * @param <K> the type of the old key before applying the error capturer
     * @param <V> the type of the old value before applying the error capturer
     * @param <KR> the type of the new key after applying the error capturer
     * @param <VR> the type of the new value after applying the error capturer
     * @return A single key-value pair containing the old key and a {@link ProcessingError} if an exception was thrown
     * upon processing the input key-value pair or an empty list
     */
    static <K, V, KR, VR> Iterable<KeyValue<K, ProcessingError<V>>> getErrors(
            @SuppressWarnings("unused") final KR newKey, final ProcessedKeyValue<K, V, ? extends VR> recordWithOldKey) {
        return recordWithOldKey.getErrors();
    }

    /**
     * Extract errors from a processed key-value. If an error is available, it will give information about the input
     * key, value, and the {@link Exception} that was thrown while attempting to map it to a new value.
     *
     * @return A single key-value pair containing the old key and a {@link ProcessingError} if an exception was thrown
     * upon processing the input key-value pair or an empty list
     */
    Iterable<KeyValue<K, ProcessingError<V>>> getErrors();

    /**
     * Extract successfully processed values from a processed key-value.
     *
     * @return A single value if processing was successful or an empty list;
     */
    Iterable<VR> getValues();
}
