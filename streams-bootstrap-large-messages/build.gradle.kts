description = "Utils for using Large Message SerDe with your Kafka Streams Application"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(platform("com.bakdata.kafka:large-message-bom:2.10.2-SNAPSHOT"))
    implementation(group = "com.bakdata.kafka", name = "large-message-core")
}
