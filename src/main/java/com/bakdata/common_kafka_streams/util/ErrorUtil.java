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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.util.Objects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.compress.utils.Charsets;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;

/**
 * This class provides utility methods for dealing with errors in Kafka streams, such as serializing values to string
 * and classifying errors as recoverable.
 */
@Slf4j
@UtilityClass
public class ErrorUtil {

    private static final String ORG_APACHE_KAFKA_COMMON_ERRORS = "org.apache.kafka.common.errors";

    /**
     * Check if an exception is recoverable and thus should be thrown so that the process is restarted by the execution
     * environment.
     * <p>Recoverable errors are:
     * <ul>
     *     <li>{@link #isRecoverableKafkaError(Exception)}
     * </ul>
     *
     * @param e exception
     * @return whether exception is recoverable or not
     */
    public static boolean isRecoverable(final Exception e) {
        return isRecoverableKafkaError(e);
    }

    /**
     * Check if an exception is thrown by Kafka, i.e., located in package {@code org.apache.kafka.common.errors}, and is
     * recoverable.
     * <p>Non-recoverable Kafka errors are:
     * <ul>
     *     <li>{@link RecordTooLargeException}
     *     <li>{@link SerializationException} which is not caused by timeout
     * </ul>
     *
     * @param e exception
     * @return whether exception is thrown by Kafka and recoverable or not
     */
    public static boolean isRecoverableKafkaError(final Exception e) {
        if (ORG_APACHE_KAFKA_COMMON_ERRORS.equals(e.getClass().getPackageName())) {
            if (e instanceof RecordTooLargeException) {
                return false;
            }
            if (e instanceof SerializationException) {
                // socket timeouts usually indicate that the schema registry is temporarily down
                return e.getCause() instanceof SocketTimeoutException;
            }
            return true;
        }
        return false;
    }

    /**
     * Convert an object to {@code String}. {@code SpecificRecord} will be serialized using {@link
     * #toString(SpecificRecord)}.
     *
     * @param o object to be serialized
     * @return {@code String} representation of record
     */
    public static String toString(final Object o) {
        final Object o1 = o instanceof SpecificRecord ? toString((SpecificRecord) o) : o;
        return Objects.toString(o1);
    }

    /**
     * Convert a {@code SpecificRecord} to {@code String} using JSON serialization.
     *
     * @param record record to be serialized
     * @return JSON representation of record or record if an error occured
     */
    private static Object toString(final SpecificRecord record) {
        try {
            return writeAsJson(record);
        } catch (final IOException ex) {
            log.warn("Failed to write to json", ex);
            return record;
        }
    }

    private static String writeAsJson(final SpecificRecord itemRecord) throws IOException {
        final Schema targetSchema = itemRecord.getSchema();
        final DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(targetSchema);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeJson(itemRecord, writer, out);
            return new String(out.toByteArray(), Charsets.ISO_8859_1);
        }
    }

    private static <T extends GenericContainer> void writeJson(final T itemRecord, final DatumWriter<T> writer,
            final OutputStream out) throws IOException {
        final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(itemRecord.getSchema(), out);
        writer.write(itemRecord, jsonEncoder);
        jsonEncoder.flush();
    }
}
