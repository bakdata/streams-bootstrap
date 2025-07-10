description = "Collection of commonly used modules when writing a Kafka Streams Application"

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(platform(libs.kafka.bom)) // Central repository requires this as a direct dependency to resolve versions
    api(libs.kafka.streams.utils)
    implementation(libs.kafka.tools)

    api(libs.kafka.streams)
    api(libs.kafka.clients)
    implementation(libs.slf4j)
    implementation(libs.jool)
    implementation(libs.resilience4j.retry)
    api(platform(libs.errorHandling.bom))
    api(libs.errorHandling.core)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.junit.pioneer)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)

    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(project(":streams-bootstrap-schema-registry"))
    testImplementation(libs.kafka.streams.avro.serde) {
        exclude(group = "org.apache.kafka", module = "kafka-clients") // force usage of OSS kafka-clients
    }
    testImplementation(libs.log4j.slf4j2)
}

tasks.withType<Test> {
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
    maxHeapSize = "4g"
}
