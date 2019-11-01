package com.bakdata.common_kafka_streams.util;

/**
 * A processed value is created upon capturing errors in a streams topology. It can either contain a successfully
 * processed value or a {@link ProcessingError} describing the input value and the {@link Exception} that has been
 * thrown.
 *
 * @param <V> the type of the old value before applying the error capturer
 * @param <VR> the type of the new value after applying the error capturer
 */
public interface ProcessedValue<V, VR> {

    /**
     * Extract errors from a processed value. If an error is available, it will give information about the input value
     * and the {@link Exception} that was thrown while attempting to map it to a new value.
     *
     * @return A single {@link ProcessingError} if an exception was thrown upon processing the input value or an empty
     * list
     */
    Iterable<ProcessingError<V>> getErrors();

    /**
     * Extract successfully processed values from a processed value.
     *
     * @return A single value if processing was successful or an empty list
     */
    Iterable<VR> getValues();
}
