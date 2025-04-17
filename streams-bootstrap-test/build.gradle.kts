description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.fluentKafkaStreamsTests)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testFixturesApi(libs.testcontainers.junit)
    testFixturesApi(libs.testcontainers.kafka)
    testImplementation(libs.log4j.slf4j2)
    testFixturesApi(libs.awaitility)
}
