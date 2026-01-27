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

package com.bakdata.kafka.streams;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Configuration for a {@link com.bakdata.kafka.streams.kstream.StreamsBuilderX}. Specifies how to build a
 * {@link org.apache.kafka.streams.Topology}.
 */
public class StreamsBuilderXConfig extends AbstractConfig {

    private static final String PREFIX = "streams.bootstrap.";
    public static final String LINEAGE_ENABLED_CONFIG = PREFIX + "lineage.enabled";
    private static final String LINEAGE_ENABLED_DOC =
            "Add headers containing lineage information to each record read from a topic";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(LINEAGE_ENABLED_CONFIG, Type.BOOLEAN, false, Importance.LOW, LINEAGE_ENABLED_DOC);

    /**
     * Create a new {@code StreamsBuilderXConfig} using the given properties.
     *
     * @param properties properties that specify how to build a {@link org.apache.kafka.streams.Topology}
     */
    public StreamsBuilderXConfig(final Map<String, Object> properties) {
        super(CONFIG_DEF, properties);
    }

    /**
     * Check if adding lineage headers is enabled. This is controlled by {@link #LINEAGE_ENABLED_CONFIG}
     *
     * @return true if lineage headers are added to streams and tables
     */
    public boolean isLineageEnabled() {
        return this.getBoolean(LINEAGE_ENABLED_CONFIG);
    }
}
