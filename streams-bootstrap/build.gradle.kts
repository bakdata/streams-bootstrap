description = "Collection of commonly used modules when writing a Kafka Streams Application"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    val kafkaVersion: String by project
    implementation(group = "org.apache.kafka", name = "kafka-tools", version = kafkaVersion)

    implementation(group = "info.picocli", name = "picocli", version = "4.7.5")
    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
    val confluentVersion: String by project
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    val log4jVersion = "2.23.0"
    implementation(group = "org.apache.logging.log4j", name = "log4j-core", version = log4jVersion)
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    api(
        group = "org.slf4j",
        name = "slf4j-api",
        version = "2.0.9"
    ) // required because other dependencies use Slf4j 1.x which is not properly resolved if this library is used in test scope
    implementation(group = "com.google.guava", name = "guava", version = "33.0.0-jre")
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")

    val junitVersion = "5.10.2"
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.junit-pioneer", name = "junit-pioneer", version = "2.2.0")
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.25.3")
    val mockitoVersion = "5.10.0"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    val fluentKafkaVersion: String by project
    testImplementation(project(":streams-bootstrap-test"))
    testImplementation(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = fluentKafkaVersion
    )
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.6.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }

    testImplementation(group = "com.ginsberg", name = "junit5-system-exit", version = "1.1.2")
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
}
