description = "Utils for using Large Message SerDe with your Kafka Streams Application"

dependencies {
    api(project(":streams-bootstrap"))
    implementation(group = "com.bakdata.kafka", name = "large-message-core", version = "2.5.1")
}
