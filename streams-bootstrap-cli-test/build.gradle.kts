description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":streams-bootstrap-test"))
    api(project(":streams-bootstrap-cli"))

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
    testImplementation(libs.log4j.slf4j2)
}
