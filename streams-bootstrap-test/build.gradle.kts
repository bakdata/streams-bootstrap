description = "Utils for testing your Kafka Streams Application"

dependencies {
    api(project(":streams-bootstrap-core"))
    val fluentKafkaVersion: String by project
    api(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = fluentKafkaVersion
    )
    implementation(group = "com.bakdata.seq2", name = "seq2", version = "1.0.12")
}
