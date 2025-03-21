description = "Collection of commonly used modules when writing a Kafka Streams Application"

plugins {
    id("java-library")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    api(group = "com.bakdata.kafka", name = "kafka-streams-utils", version = "1.1.1-SNAPSHOT")
    implementation(group = "org.apache.kafka", name = "kafka-tools")

    api(group = "org.apache.kafka", name = "kafka-streams")
    api(group = "org.apache.kafka", name = "kafka-clients")
    implementation(group = "io.confluent", name = "kafka-schema-serializer") {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    api(group = "io.confluent", name = "kafka-schema-registry-client") {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    implementation(
        group = "org.slf4j",
        name = "slf4j-api",
        version = "2.0.16"
    )
    implementation(group = "org.jooq", name = "jool", version = "0.9.15")
    implementation(group = "io.github.resilience4j", name = "resilience4j-retry", version = "2.3.0")
    api(platform("com.bakdata.kafka:error-handling-bom:1.8.0"))
    api(group = "com.bakdata.kafka", name = "error-handling-core")

    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.junit-pioneer", name = "junit-pioneer", version = "2.3.0")
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde") {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
    maxHeapSize = "4g"
}
