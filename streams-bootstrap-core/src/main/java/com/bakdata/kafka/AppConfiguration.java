/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import com.bakdata.kafka.admin.AdminClientX;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;

/**
 * Configuration for setting up an app
 * @param <T> type of topic config
 * @see com.bakdata.kafka.streams.StreamsApp#setup(AppConfiguration)
 * @see com.bakdata.kafka.streams.StreamsApp#setupCleanUp(AppConfiguration)
 * @see com.bakdata.kafka.producer.ProducerApp#setup(AppConfiguration)
 * @see com.bakdata.kafka.producer.ProducerApp#setupCleanUp(AppConfiguration)
 */
@Value
@EqualsAndHashCode
public class AppConfiguration<T> {
    @NonNull
    T topics;
    @NonNull
    Map<String, Object> kafkaProperties;

    /**
     * Create a new {@link AdminClientX} using {@link #kafkaProperties}
     *
     * @return {@link AdminClientX}
     */
    public AdminClientX createAdminClient() {
        return AdminClientX.create(this.kafkaProperties);
    }
}
