/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka.consumerproducer;

import com.bakdata.kafka.CloseExecutionOptions;
import com.bakdata.kafka.consumer.ConsumerExecutionOptions;
import java.time.Duration;
import lombok.Builder;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;

/**
 * Options to run a Kafka ConsumerProducer app
 */
@Builder
public final class ConsumerProducerExecutionOptions {

    @Builder.Default
    private final CloseExecutionOptions closeExecutionOptions = CloseExecutionOptions.builder().build();

    /**
     * Defines the timeout duration for the {@link Consumer#poll(Duration)} call
     */
    @Builder.Default
    @Getter
    private final Duration pollTimeout = Duration.ofMillis(100L);

    ConsumerExecutionOptions toConsumerExecutionOptions() {
        return ConsumerExecutionOptions.builder()
                .closeExecutionOptions(this.closeExecutionOptions)
                .pollTimeout(this.pollTimeout)
                .build();
    }
}
