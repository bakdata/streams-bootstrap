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
    api(
        group = "org.slf4j",
        name = "slf4j-api",
        version = "2.0.9"
    ) // required because other dependencies use Slf4j 1.x which is not properly resolved if this library is used in test scope
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")

    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.junit-pioneer", name = "junit-pioneer", version = "2.2.0")
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    val fluentKafkaVersion: String by project
    testImplementation(project(":streams-bootstrap-test"))
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )
    val testContainersVersion: String by project
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testImplementation(group = "org.testcontainers", name = "kafka", version = testContainersVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
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
