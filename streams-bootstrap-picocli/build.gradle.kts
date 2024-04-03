description = "Base classes to create standalone Java applications using picocli"

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    api(project(":streams-bootstrap"))
    api(group = "info.picocli", name = "picocli", version = "4.7.5")
    val log4jVersion: String by project
    implementation(group = "org.apache.logging.log4j", name = "log4j-core", version = log4jVersion)
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    val kafkaJunitVersion: String by project
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaJunitVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "com.ginsberg", name = "junit5-system-exit", version = "1.1.2")
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
}
