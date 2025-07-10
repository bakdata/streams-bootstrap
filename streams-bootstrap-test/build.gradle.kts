description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.fluentKafkaStreamsTests)
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
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testImplementation(libs.kafka.protobuf.provider) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testFixturesApi(libs.testcontainers.junit)
    testFixturesApi(libs.testcontainers.kafka)
    testImplementation(libs.log4j.slf4j2)
    testFixturesApi(libs.awaitility)
    testFixturesImplementation(libs.kafka.schema.serializer) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
}
