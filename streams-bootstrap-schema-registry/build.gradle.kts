description = "Utils for using Confluent Schema Registry with your Kafka Streams Application"

dependencies {
    api(project(":streams-bootstrap-core"))
    val confluentVersion: String by project
    api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
    implementation(group = "io.confluent", name = "kafka-schema-serializer", version = confluentVersion)
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
}
