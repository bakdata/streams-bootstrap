/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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

package com.bakdata.kafka;

import java.util.function.Function;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@code KafkaProducerApplication} without any additional configuration options.
 */
@RequiredArgsConstructor
public final class SimpleKafkaProducerApplication extends KafkaProducerApplication {
    private final @NonNull Function<Boolean, ProducerApp> appFactory;

    /**
     * Create new {@code SimpleKafkaProducerApplication}
     * @param appFactory factory to create {@code ProducerApp} without any parameters
     */
    public SimpleKafkaProducerApplication(final Supplier<? extends ProducerApp> appFactory) {
        this(cleanUp -> appFactory.get());
    }

    @Override
    public ProducerApp createApp(final boolean cleanUp) {
        return this.appFactory.apply(cleanUp);
    }
}