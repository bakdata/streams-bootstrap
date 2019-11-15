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

package com.bakdata.common_kafka_streams.s3backed;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

@NoArgsConstructor
@Slf4j
public class S3BackedSerializer<T> implements Serializer<T> {
    public static final byte IS_NOT_BACKED = 0;
    public static final byte IS_BACKED = 1;
    private static final String VALUE_PREFIX = "values";
    private static final String KEY_PREFIX = "keys";
    private AmazonS3 s3;
    private Serializer<? super T> serializer;
    private AmazonS3URI basePath;
    private int maxSize;
    private boolean isKey;

    private static String toString(final String s) {
        return s == null ? "" : s;
    }

    private static byte[] serialize(final byte[] uriBytes, final byte flag) {
        final byte[] fullBytes = prepareBytes(uriBytes);
        fullBytes[0] = flag;
        return fullBytes;
    }

    private static byte[] prepareBytes(final byte[] bytes) {
        final byte[] fullBytes = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, fullBytes, 1, bytes.length);
        return fullBytes;
    }

    static byte[] serialize(final String uri) {
        final byte[] uriBytes = uri.getBytes();
        return serialize(uriBytes, IS_BACKED);
    }

    static byte[] serialize(final byte[] bytes) {
        return serialize(bytes, IS_NOT_BACKED);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedSerdeConfig serdeConfig = new S3BackedSerdeConfig(configs);
        final Serde<T> serde = isKey ? serdeConfig.getKeySerde() : serdeConfig.getValueSerde();
        this.serializer = serde.serializer();
        this.maxSize = serdeConfig.getMaxSize();
        this.basePath = serdeConfig.getBasePath();
        this.s3 = serdeConfig.getS3();
        this.serializer.configure(configs, isKey);
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        Objects.requireNonNull(this.serializer);
        final byte[] bytes = this.serializer.serialize(topic, data);
        if (this.needsS3Backing(bytes)) {
            Objects.requireNonNull(this.basePath);
            Objects.requireNonNull(this.s3);
            final String key = this.createS3Key(topic, this.getPrefix());
            final String uri = this.uploadToS3(bytes, key);
            return serialize(uri);
        } else {
            return serialize(bytes);
        }
    }

    @Override
    public void close() {
        this.serializer.close();
    }

    private String getPrefix() {
        return this.isKey ? KEY_PREFIX : VALUE_PREFIX;
    }

    private String createS3Key(final @NonNull String topic, final String prefix) {
        return toString(this.basePath.getKey()) + topic + "/" + prefix + "/" + UUID.randomUUID();
    }

    private String uploadToS3(final byte[] bytes, final String key) {
        final String bucket = this.basePath.getBucket();
        try (final InputStream content = new ByteArrayInputStream(bytes)) {
            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(bytes.length);
            this.s3.putObject(bucket, key, content, metadata);
            final String uri = "s3://" + bucket + "/" + key;
            log.info("Stored large object on S3: {}", uri);
            return uri;
        } catch (final IOException e) {
            throw new RuntimeException("Error uploading object to S3", e);
        }
    }

    private boolean needsS3Backing(final byte[] bytes) {
        return bytes.length >= this.maxSize;
    }
}
