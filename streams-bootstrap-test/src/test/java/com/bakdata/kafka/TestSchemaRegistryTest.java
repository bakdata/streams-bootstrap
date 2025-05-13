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

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class TestSchemaRegistryTest {

    private static int getLatestVersion(final SchemaRegistryClient client, final String subject)
            throws IOException, RestClientException {
        final List<Integer> versions = client.getAllVersions(subject);
        return versions.get(versions.size() - 1);
    }

    @Test
    void shouldUseRandomScope() {
        assertThat(new TestSchemaRegistry().getSchemaRegistryUrl()).startsWith("mock://");
    }

    @Test
    void shouldCreateClient() throws RestClientException, IOException {
        try (final SchemaRegistryClient client = new TestSchemaRegistry().getSchemaRegistryClient()) {
            assertThat(client.getAllSubjects()).isEmpty();
        }
    }

    @Test
    void shouldUseUrl() throws RestClientException, IOException {
        final Schema schema = SchemaBuilder.record("MyRecord")
                .fields()
                .endRecord();
        final ParsedSchema parsedSchema = new AvroSchema(schema);
        try (final SchemaRegistryClient client = new TestSchemaRegistry("mock://").getSchemaRegistryClient()) {
            client.register("subject", parsedSchema);
        }
        try (final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope("")) {
            final int latestVersion = getLatestVersion(client, "subject");
            assertThat(client.getSchemaById(latestVersion))
                    .isEqualTo(parsedSchema);
        }
    }

    @Test
    void shouldUseProviders() throws RestClientException, IOException {
        final ParsedSchema parsedSchema = new ProtobufSchema("message MyMessage {}");
        final List<SchemaProvider> providers = List.of(new ProtobufSchemaProvider());
        final TestSchemaRegistry testSchemaRegistry = new TestSchemaRegistry();
        final String url = testSchemaRegistry.getSchemaRegistryUrl();
        final String scope = MockSchemaRegistry.validateAndMaybeGetMockScope(List.of(url));
        try (final SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(scope, providers)) {
            client.register("subject", parsedSchema);
        }
        try (final SchemaRegistryClient client = testSchemaRegistry.getSchemaRegistryClient(providers)) {
            final int latestVersion = getLatestVersion(client, "subject");
            final io.confluent.kafka.schemaregistry.client.rest.entities.Schema schema =
                    client.getByVersion("subject", latestVersion, false);
            assertThat(client.parseSchema(schema)).hasValue(parsedSchema);
        }
    }

}
