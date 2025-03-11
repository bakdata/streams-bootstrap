description = "BOM for Kafka."

plugins {
    id("java-platform")
}

javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        val kafkaVersion: String by project
        api(group = "org.apache.kafka", name = "kafka-tools", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
        val confluentVersion: String by project
        api(group = "io.confluent", name = "kafka-schema-serializer", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
    }
}
