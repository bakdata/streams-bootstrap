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
        api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-server", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-server-common", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-tools", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "connect-transforms", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "connect-json", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
        val confluentVersion: String by project
        api(group = "io.confluent", name = "kafka-schema-serializer", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-avro-serializer", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-protobuf-serializer", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-protobuf-provider", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-json-schema-serializer", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-json-schema-provider", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-streams-json-schema-serde", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-connect-avro-converter", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-connect-protobuf-converter", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-connect-json-schema-converter", version = confluentVersion)
    }
}
