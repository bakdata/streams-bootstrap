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

import static java.util.Collections.emptyMap;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Class that provides helpers for using schema registry in tests.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public final class TestEnvironment {

    private static final String MOCK_URL_PREFIX = "mock://";
    private final String schemaRegistryUrl;

    /**
     * Create a new {@code SchemaRegistryEnv} with no configured Schema Registry.
     * @return {@code SchemaRegistryEnv} with no configured Schema Registry
     */
    public static TestEnvironment withoutSchemaRegistry() {
        return withSchemaRegistry(null);
    }

    /**
     * Create a new {@code SchemaRegistryEnv} with configured
     * {@link io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry}. The scope is random in order to avoid
     * collisions between different test instances as scopes are retained globally.
     * @return {@code SchemaRegistryEnv} with configured Schema Registry
     */
    public static TestEnvironment withSchemaRegistry() {
        return withSchemaRegistry(MOCK_URL_PREFIX + UUID.randomUUID());
    }

    /**
     * Create a new {@code SchemaRegistryEnv} with configured Schema Registry.
     * @param schemaRegistryUrl Schema Registry URL to use
     * @return {@code SchemaRegistryEnv} with configured Schema Registry
     */
    public static TestEnvironment withSchemaRegistry(final String schemaRegistryUrl) {
        return new TestEnvironment(schemaRegistryUrl);
    }

    /**
     * Get {@code SchemaRegistryClient} for configured URL with default providers
     * @return {@code SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.getSchemaRegistryClient(null);
    }

    /**
     * Get {@code SchemaRegistryClient} for configured URL
     * @param providers list of {@code SchemaProvider} to use for {@code SchemaRegistryClient}
     * @return {@code SchemaRegistryClient}
     * @throws NullPointerException if Schema Registry is not configured
     */
    public SchemaRegistryClient getSchemaRegistryClient(final List<SchemaProvider> providers) {
        final List<String> baseUrls = List.of(
                Objects.requireNonNull(this.schemaRegistryUrl, "Schema Registry is not configured")
        );
        return SchemaRegistryClientFactory.newClient(baseUrls, 0, providers, emptyMap(), null);
    }
}
