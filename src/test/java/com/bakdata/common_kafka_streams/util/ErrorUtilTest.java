/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata
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

package com.bakdata.common_kafka_streams.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.net.SocketTimeoutException;
import java.util.stream.Stream;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ErrorUtilTest {

    static Stream<Arguments> generateConvertToStringParameters() {
        return Stream.of(
                Arguments.of(1, "1"),
                Arguments.of(null, "null"),
                Arguments.of(ErrorDescription.newBuilder().setMessage("foo").setStackTrace("bar").build(),
                        "{\"message\":{\"string\":\"foo\"},\"stack_trace\":{\"string\":\"bar\"}}")
        );
    }

    static Stream<Arguments> generateIsRecoverableExceptionParameters() {
        return Stream.of(
                Arguments.of(mock(Exception.class), false),
                Arguments.of(new SerializationException(new SocketTimeoutException()), true),
                Arguments.of(new SerializationException(mock(Exception.class)), false)
        );
    }

    @ParameterizedTest
    @MethodSource("generateConvertToStringParameters")
    void shouldConvertToString(final Object object, final String expected) {
        assertThat(ErrorUtil.toString(object))
                .as("Convert %s", object)
                .isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("generateIsRecoverableExceptionParameters")
    void shouldClassifyRecoverableErrors(final Exception exception, final boolean isRecoverable) {
        assertThat(ErrorUtil.isRecoverable(exception))
                .isEqualTo(isRecoverable);
    }
}