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
import org.apache.kafka.common.errors.SerializationException;

@Slf4j
@UtilityClass
public class ErrorUtil {

    private static String writeAsJson(final SpecificRecord itemRecord) throws IOException {
        final Schema targetSchema = itemRecord.getSchema();
        final DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(targetSchema);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeJson(itemRecord, writer, out);
            return new String(out.toByteArray());
        }
    }

    private static <T extends GenericContainer> void writeJson(final T itemRecord, final DatumWriter<T> writer,
            final OutputStream out) throws IOException {
        final JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(itemRecord.getSchema(), out);
        writer.write(itemRecord, jsonEncoder);
        jsonEncoder.flush();
    }

    private static Object toString(final SpecificRecord value) {
        try {
            return writeAsJson(value);
        } catch (final IOException ex) {
            log.warn("Failed to write to json", ex);
            return value;
        }
    }

    public static String toString(final Object o) {
        final Object o1 = o instanceof SpecificRecord ? toString((SpecificRecord) o) : o;
        return Objects.toString(o1);
    }

    public static boolean shouldForwardError(final Exception e) {
        return isSchemaRegistryTimeout(e);
    }

    private static boolean isSchemaRegistryTimeout(final Exception e) {
        return e instanceof SerializationException && e.getCause() instanceof SocketTimeoutException;
    }
}
