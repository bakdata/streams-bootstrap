package com.bakdata.common_kafka_streams.util;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Base class for decorating a {@code ProcessorContext}
 */
@RequiredArgsConstructor
public abstract class DecoratorProcessorContext implements ProcessorContext {
    @Delegate
    private final @NonNull ProcessorContext wrapped;
}
