description = "Utils for using Large Message SerDe with your Kafka Streams Application"

dependencies {
    api(project(":streams-bootstrap-core"))
    implementation(group = "com.bakdata.kafka", name = "large-message-core", version = "2.7.0")
}
