description = "Utils for testing your Kafka Streams Application"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-test"))
    api(project(":streams-bootstrap-cli"))
}
