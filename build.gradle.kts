description = "Collection of commonly used modules when writing a Kafka Streams Application"


plugins {
    `java-library`
    id("net.researchgate.release") version "2.8.1"
    id("com.bakdata.sonar") version "1.1.7"
    id("com.bakdata.sonatype") version "1.1.7"
    id("org.hildan.github.changelog") version "0.8.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
    id("io.freefair.lombok") version "5.3.3.3"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 1 // Embedded Kafka does not reliably work in parallel since Kafka 3.0
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Lawrence Benson")
            id.set("lawben")
        }
        developer {
            name.set("Benjamin Feldmann")
            id.set("BJennWare")
        }
        developer {
            name.set("Arvid Heise")
            id.set("AHeise")
        }
        developer {
            name.set("Victor Künstler")
            id.set("VictorKuenstler")
        }
        developer {
            name.set("Sven Lehmann")
            id.set("SvenLehmann")
        }
        developer {
            name.set("Torben Meyer")
            id.set("torbsto")
        }
        developer {
            name.set("Fabian Paul")
            id.set("fapaul")
        }
        developer {
            name.set("Yannick Roeder")
            id.set("yannick-roeder")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

allprojects {

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    dependencies {
        val kafkaVersion: String by project
        implementation(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion)

        implementation(group = "info.picocli", name = "picocli", version = "4.6.1")
        api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
        val confluentVersion: String by project
        implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
        val log4jVersion = "2.17.1"
        implementation(group = "org.apache.logging.log4j", name = "log4j-core", version = log4jVersion)
        implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
        implementation(group = "com.google.guava", name = "guava", version = "30.1.1-jre")
        implementation(group = "org.jooq", name = "jool", version = "0.9.14")

        val junitVersion = "5.7.2"
        testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
        testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
        testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
        testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
        val mockitoVersion = "3.12.4"
        testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
        testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

        val fluentKafkaVersion = "2.5.6-SNAPSHOT"
        testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = fluentKafkaVersion)
        testImplementation(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
        testImplementation(
            group = "com.bakdata.fluent-kafka-streams-tests",
            name = "schema-registry-mock-junit5",
            version = fluentKafkaVersion
        )
        testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.1.0") {
            exclude(group = "org.slf4j", module = "slf4j-log4j12")
        }

        testImplementation(group = "com.ginsberg", name = "junit5-system-exit", version = "1.1.1")
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

tasks.withType<Test> {
    useJUnitPlatform()
}
