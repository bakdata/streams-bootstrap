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
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ProcessingErrorTest {

    @Mock
    Throwable throwable;

    @Test
    void shouldCreateDeadLetter() {
        when(this.throwable.getMessage()).thenReturn("baz");
        doAnswer(i -> {
            final PrintWriter writer = i.getArgument(0);
            writer.print("qux");
            return null;
        }).when(this.throwable).printStackTrace(any(PrintWriter.class));
        final ProcessingError<Object> error = ProcessingError.builder()
                .value("foo")
                .throwable(this.throwable)
                .build();
        assertThat(error.createDeadLetter("bar"))
                .isNotNull()
                .satisfies(deadLetter -> {
                    assertThat(deadLetter.getInputValue()).isEqualTo("foo");
                    assertThat(deadLetter.getDescription()).isEqualTo("bar");
                    assertThat(deadLetter.getCause()).satisfies(cause -> {
                        assertThat(cause.getMessage()).isEqualTo("baz");
                        assertThat(cause.getStackTrace()).isEqualTo("qux");
                    });
                });
    }

    @Test
    void shouldNotAllowNullCause() {
        assertThatNullPointerException()
                .isThrownBy(() -> ProcessingError.builder().value("foo").throwable(null).build());
    }

    @Test
    void shouldAllowNullValue() {
        assertThat(ProcessingError.builder().value(null).throwable(this.throwable).build())
                .isNotNull();
    }

    @Test
    void shouldNotAllowNullDescription() {
        final ProcessingError<Object> error = ProcessingError.builder().value("foo").throwable(this.throwable).build();
        assertThatNullPointerException().isThrownBy(() -> error.createDeadLetter(null));
    }

}