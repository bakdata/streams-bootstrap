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

import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ConfiguratorTest {

    @InjectSoftAssertions
    private SoftAssertions softly;
    @Mock
    private Serde<String> serde;
    @Mock
    private Serializer<String> serializer;

    @Test
    void shouldConfigureValueSerde() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForValues(this.serde)).isEqualTo(this.serde);
        verify(this.serde).configure(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ), false);
    }

    @Test
    void shouldConfigureValueSerdeWithConfig() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForValues(this.serde, Map.of(
                "prop2", "overridden",
                "prop3", "value3"
        ))).isEqualTo(this.serde);
        verify(this.serde).configure(Map.of(
                "prop1", "value1",
                "prop2", "overridden",
                "prop3", "value3"
        ), false);
    }

    @Test
    void shouldConfigureKeySerde() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForKeys(this.serde)).isEqualTo(this.serde);
        verify(this.serde).configure(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ), true);
    }

    @Test
    void shouldConfigureKeySerdeWithConfig() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForKeys(this.serde, Map.of(
                "prop2", "overridden",
                "prop3", "value3"
        ))).isEqualTo(this.serde);
        verify(this.serde).configure(Map.of(
                "prop1", "value1",
                "prop2", "overridden",
                "prop3", "value3"
        ), true);
    }

    @Test
    void shouldConfigureValueSerializer() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForValues(this.serializer)).isEqualTo(this.serializer);
        verify(this.serializer).configure(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ), false);
    }

    @Test
    void shouldConfigureValueSerializerWithConfig() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForValues(this.serializer, Map.of(
                "prop2", "overridden",
                "prop3", "value3"
        ))).isEqualTo(this.serializer);
        verify(this.serializer).configure(Map.of(
                "prop1", "value1",
                "prop2", "overridden",
                "prop3", "value3"
        ), false);
    }

    @Test
    void shouldConfigureKeySerializer() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForKeys(this.serializer)).isEqualTo(this.serializer);
        verify(this.serializer).configure(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ), true);
    }

    @Test
    void shouldConfigureKeySerializerWithConfig() {
        final Configurator configurator = new Configurator(Map.of(
                "prop1", "value1",
                "prop2", "value2"
        ));
        this.softly.assertThat(configurator.configureForKeys(this.serializer, Map.of(
                "prop2", "overridden",
                "prop3", "value3"
        ))).isEqualTo(this.serializer);
        verify(this.serializer).configure(Map.of(
                "prop1", "value1",
                "prop2", "overridden",
                "prop3", "value3"
        ), true);
    }

}
