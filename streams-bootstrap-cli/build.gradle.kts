description = "Base classes to create standalone Java applications using picocli"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.picocli)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(project(":streams-bootstrap-core")))
    testImplementation(libs.junit.systemExit)
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testImplementation(libs.log4j.slf4j2)
}
