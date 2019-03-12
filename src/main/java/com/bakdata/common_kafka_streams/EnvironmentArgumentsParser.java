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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.StringJoiner;


/**
 * <p>This class is primarily used to inject environment variables to the passed in command line arguments in {@link KafkaStreamsApplication}.</p>
 *
 * <p>In general a usage would look like this:</p>
 * <pre>{@code
 * final String[] environmentArguments = new EnvironmentArgumentsParser(ENV_PREFIX).parseVariables(System.getenv());
 * }</pre>
 * The class collects all environment variables starting with {@link #environmentPrefix} and replaces the  {@link #environmentDelimiter} with the {@link #commandLineDelimiter}.
 * Furthermore it transforms all words to lowercase and prepends "--" to match the command line argument descriptors.
 * <p>Example:</p>
 * {@code
 *  var ENV_PREFIX = "APP_";
 *  Transformation: APP_INPUT_TOPIC --> --input-topic
 * }
 */
public class EnvironmentArgumentsParser {

    private final String environmentPrefix;

    private final String commandLineDelimiter;

    private final String environmentDelimiter;

    public EnvironmentArgumentsParser(final String environmentPrefix) {
        this(environmentPrefix, "-", "_");
    }

    public EnvironmentArgumentsParser(final String environmentPrefix, final String commandLineDelimiter,
            final String environmentDelimiter) {
        this.environmentPrefix = environmentPrefix;
        this.commandLineDelimiter = commandLineDelimiter;
        this.environmentDelimiter = environmentDelimiter;
    }

    public String[] parseVariables(final Map<String, String> environment) {
        final Collection<String> environmentArguments = new ArrayList<>();
        environment.forEach((k, v) -> {
            if (!k.startsWith(this.environmentPrefix)) {
                return;
            }
            environmentArguments.add(this.convertEnvironmentKeyToCommandLineParameter(k));
            environmentArguments.add(v);
        });
        return environmentArguments.toArray(new String[0]);
    }


    private String convertEnvironmentKeyToCommandLineParameter(final String environmentKey) {
        final StringJoiner sj = new StringJoiner(this.commandLineDelimiter);
        final String[] words = environmentKey.split(this.environmentPrefix)[1].split(this.environmentDelimiter);
        for (final String word : words) {
            sj.add(word.toLowerCase());
        }
        return "--" + sj;
    }
}
