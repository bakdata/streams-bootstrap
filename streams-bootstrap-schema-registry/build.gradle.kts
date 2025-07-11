description = "Utils for using Confluent Schema Registry with your Kafka Streams Application"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.kafka.schema.registry.client) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
        exclude(group = "org.slf4j", module = "slf4j-api") // Conflict with 2.x when used as dependency
    }
    implementation(libs.kafka.schema.serializer) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
        exclude(group = "org.slf4j", module = "slf4j-api") // Conflict with 2.x when used as dependency
    }
    implementation(libs.slf4j)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
}
