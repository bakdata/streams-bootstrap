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

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A pre-configured {@link Serde} or {@link Serializer}, i.e., configs and isKey are set.
 * @param <T> type of underlying configurable
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Preconfigured<T> {
    private final @NonNull Configurable<T> configurable;
    private final @NonNull Map<String, Object> configOverrides;
    private final boolean isKey;

    /**
     * Pre-configure a {@code Serde} for values
     * @param serde {@code Serde} to pre-configure
     * @return pre-configured serde
     * @param <S> type of {@link Serde}
     * @param <T> type (de-)serialized by the {@code Serde}
     */
    public static <S extends Serde<T>, T> Preconfigured<S> value(final S serde) {
        return value(configurable(serde));
    }

    /**
     * Pre-configure a {@code Serde} for values with config overrides
     * @param serde {@code Serde} to pre-configure
     * @param configOverrides configs passed to {@link Serde#configure(Map, boolean)}
     * @return pre-configured serde
     * @param <S> type of {@link Serde}
     * @param <T> type (de-)serialized by the {@code Serde}
     */
    public static <S extends Serde<T>, T> Preconfigured<S> value(final S serde,
            final Map<String, Object> configOverrides) {
        return value(configurable(serde), configOverrides);
    }

    /**
     * Pre-configure a {@code Serde} for keys
     * @param serde {@code Serde} to pre-configure
     * @return pre-configured serde
     * @param <S> type of {@link Serde}
     * @param <T> type (de-)serialized by the {@code Serde}
     */
    public static <S extends Serde<T>, T> Preconfigured<S> key(final S serde) {
        return key(configurable(serde));
    }

    /**
     * Pre-configure a {@code Serde} for keys with config overrides
     * @param serde {@code Serde} to pre-configure
     * @param configOverrides configs passed to {@link Serde#configure(Map, boolean)}
     * @return pre-configured serde
     * @param <S> type of {@link Serde}
     * @param <T> type (de-)serialized by the {@code Serde}
     */
    public static <S extends Serde<T>, T> Preconfigured<S> key(final S serde,
            final Map<String, Object> configOverrides) {
        return key(configurable(serde), configOverrides);
    }

    /**
     * Pre-configure a {@code Serializer} for values
     * @param serializer {@code Serializer} to pre-configure
     * @return pre-configured serializer
     * @param <S> type of {@link Serializer}
     * @param <T> type serialized by the {@code Serializer}
     */
    public static <S extends Serializer<T>, T> Preconfigured<S> value(final S serializer) {
        return value(configurable(serializer));
    }

    /**
     * Pre-configure a {@code Serializer} for values
     * @param serializer {@code Serializer} to pre-configure
     * @param configOverrides configs passed to {@link Serializer#configure(Map, boolean)}
     * @return pre-configured serializer
     * @param <S> type of {@link Serializer}
     * @param <T> type serialized by the {@code Serializer}
     */
    public static <S extends Serializer<T>, T> Preconfigured<S> value(final S serializer,
            final Map<String, Object> configOverrides) {
        return value(configurable(serializer), configOverrides);
    }

    /**
     * Pre-configure a {@code Serializer} for keys
     * @param serializer {@code Serializer} to pre-configure
     * @return pre-configured serializer
     * @param <S> type of {@link Serializer}
     * @param <T> type serialized by the {@code Serializer}
     */
    public static <S extends Serializer<T>, T> Preconfigured<S> key(final S serializer) {
        return key(configurable(serializer));
    }

    /**
     * Pre-configure a {@code Serializer} for keys
     * @param serializer {@code Serializer} to pre-configure
     * @param configOverrides configs passed to {@link Serializer#configure(Map, boolean)}
     * @return pre-configured serializer
     * @param <S> type of {@link Serializer}
     * @param <T> type serialized by the {@code Serializer}
     */
    public static <S extends Serializer<T>, T> Preconfigured<S> key(final S serializer,
            final Map<String, Object> configOverrides) {
        return key(configurable(serializer), configOverrides);
    }

    private static <S extends Serde<T>, T> ConfigurableSerde<S, T> configurable(final S serde) {
        return new ConfigurableSerde<>(serde);
    }

    private static <S extends Serializer<T>, T> ConfigurableSerializer<S, T> configurable(final S serializer) {
        return new ConfigurableSerializer<>(serializer);
    }

    private static <T> Preconfigured<T> key(final Configurable<T> configurable) {
        return key(configurable, emptyMap());
    }

    private static <T> Preconfigured<T> key(final Configurable<T> configurable,
            final Map<String, Object> configOverrides) {
        return new Preconfigured<>(configurable, configOverrides, true);
    }

    private static <T> Preconfigured<T> value(final Configurable<T> configurable) {
        return value(configurable, emptyMap());
    }

    private static <T> Preconfigured<T> value(final Configurable<T> configurable,
            final Map<String, Object> configOverrides) {
        return new Preconfigured<>(configurable, configOverrides, false);
    }

    /**
     * Configure using a base config
     * @param baseConfig Base config. {@link #configOverrides} override properties of base config.
     * @return configured instance
     */
    public T configure(final Map<String, Object> baseConfig) {
        final Map<String, Object> serializerConfig = this.mergeConfig(baseConfig);
        return this.configurable.configure(serializerConfig, this.isKey);
    }

    private Map<String, Object> mergeConfig(final Map<String, Object> baseConfig) {
        final Map<String, Object> config = new HashMap<>(baseConfig);
        config.putAll(this.configOverrides);
        return unmodifiableMap(config);
    }

}
