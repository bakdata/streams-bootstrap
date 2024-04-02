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

import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Parse configuration properties of a Kafka Streams app from environment variables
 */
public final class EnvironmentKafkaConfigParser {

    static final String PREFIX = "KAFKA_";
    private static final Pattern UNDERSCORE = Pattern.compile("_");
    private static final Pattern PREFIX_PATTERN = Pattern.compile("^" + PREFIX);

    private EnvironmentKafkaConfigParser() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * Parse a list of environment variables as a streams configuration. All variables starting with {@code KAFKA_}
     * prefix are converted. {@code _} are replaced by {@code .}
     *
     * @param environment map of environment variables
     * @return parsed streams configuration
     */
    public static Map<String, String> parseVariables(final Map<String, String> environment) {
        return environment.entrySet().stream()
                .filter(e -> e.getKey().startsWith(PREFIX))
                .collect(Collectors.toMap(EnvironmentKafkaConfigParser::convertEnvironmentVariable, Entry::getValue));
    }

    private static String convertEnvironmentVariable(final Entry<String, String> environmentEntry) {
        final String key = environmentEntry.getKey();
        final String withoutPrefix = PREFIX_PATTERN.matcher(key).replaceAll("");
        return UNDERSCORE.matcher(withoutPrefix).replaceAll(".")
                .toLowerCase(Locale.getDefault());
    }

}
