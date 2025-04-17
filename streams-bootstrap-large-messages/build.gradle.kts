description = "Utils for using Large Message SerDe with your Kafka Streams Application"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(platform(libs.largeMessage.bom))
    implementation(libs.largeMessage.core)
}
