description = "Collection of commonly used modules when writing a Kafka Streams Application"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "kafka-tools", version = kafkaVersion)

    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-schema-serializer", version = confluentVersion)
    api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    implementation(
        group = "org.slf4j",
        name = "slf4j-api",
        version = "2.0.16"
    )
    implementation(group = "org.jooq", name = "jool", version = "0.9.15")
    implementation(group = "io.github.resilience4j", name = "resilience4j-retry", version = "1.7.1")
    api(group = "com.bakdata.kafka", name = "error-handling-core", version = "1.6.1")

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

    testFixturesApi(project(":streams-bootstrap-test"))
    val testContainersVersion: String by project
    testFixturesApi(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testFixturesApi(group = "org.testcontainers", name = "kafka", version = testContainersVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    val awaitilityVersion: String by project
    testFixturesApi(group = "org.awaitility", name = "awaitility", version = awaitilityVersion)
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
    maxHeapSize = "4g"
}
