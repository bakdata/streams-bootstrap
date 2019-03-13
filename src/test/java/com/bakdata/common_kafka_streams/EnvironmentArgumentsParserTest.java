/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.common_kafka_streams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class EnvironmentArgumentsParserTest {

    private final EnvironmentArgumentsParser parser = new EnvironmentArgumentsParser("STREAMS_");

    @Test
    void shouldFilterVariablesWithoutEnvironmentPrefix(){
        final String[] result = this.parser.parseVariables(Map.of(
                "STREAMS_test1", "a",
                "STREAMS_test2", "a",
                "STREAMSWRONG_test2", "a"
        ));
        assertEquals(4, result.length);
    }

    @Test
    void shouldLowerEnvironmentKeys() {
        final String[] result = this.parser.parseVariables(Map.of(
                "STREAMS_teST1", "a"
        ));
        assertEquals("--test1", result[0]);
    }

    @Test
    void shouldReplaceUnderscoreWithHyphen() {
        final String[] result = this.parser.parseVariables(Map.of(
                "STREAMS_teST_test_1", "a"
        ));
        assertEquals("--test-test-1", result[0]);
    }

    @Test
    void shouldPassEnvironmentValueAsCommandLineParameter() {
        final String[] result = this.parser.parseVariables(Map.of(
            "STREAMS_test1", "a"));
        assertEquals("a", result[1]);
    }

    @Test
    void shouldReturnEmptyArrayIfNoValidKeysPresent() {
        final String[] result = this.parser.parseVariables(Map.of(
                "STREAMSWRONG_test3", "a",
                "STREAMSWRONG_test2", "a"
        ));
        assertEquals(0, result.length);
    }

}
