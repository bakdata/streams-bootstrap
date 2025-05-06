description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.fluentKafkaStreamsTests)
    implementation(libs.slf4j)
}
