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

import java.util.function.Consumer;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Builder
public class StreamsHooks {
    @Builder.Default
    private final @NonNull Consumer<KafkaStreams> onStart = streams -> {};
    @Getter
    @Builder.Default
    private final @NonNull KafkaStreams.StateListener stateListener = new NoOpStateListener();
    @Getter
    @Builder.Default
    private final @NonNull StreamsUncaughtExceptionHandler uncaughtExceptionHandler =
            new DefaultStreamsUncaughtExceptionHandler();

    public void onStart(final KafkaStreams streams) {
        this.onStart.accept(streams);
    }
}
