description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":streams-bootstrap-test"))
    api(project(":streams-bootstrap-cli"))
    implementation(libs.kafka.schema.serializer) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
        exclude(group = "org.slf4j", module = "slf4j-api") // Conflict with 2.x when used as dependency
    }

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testImplementation(libs.log4j.slf4j2)
}
