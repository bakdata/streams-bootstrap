description = "Collection of commonly used modules when writing a Kafka Streams Application"


plugins {
    `java-library`
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.1.6"
    id("com.bakdata.sonatype") version "1.1.6"
    id("org.hildan.github.changelog") version "0.8.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.0"
    id("io.freefair.lombok") version "5.1.1"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
    }

    repositories {
        mavenCentral()
        maven(url = "http://packages.confluent.io/maven/")
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

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    dependencies {
        val kafkaVersion: String by project
        implementation(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion)

        implementation(group = "info.picocli", name = "picocli", version = "4.0.4")
        api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)
        val confluentVersion: String by project
        implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
        api(group = "io.confluent", name = "kafka-schema-registry-client", version = confluentVersion)
        implementation(group = "org.apache.logging.log4j", name = "log4j-core", version = "2.14.1")
        implementation(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.25")
        implementation(group = "com.google.guava", name = "guava", version = "30.1.1-jre")
        implementation(group = "org.jooq", name = "jool", version = "0.9.14")

        val junitVersion = "5.4.0"
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
        testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")
        val mockitoVersion = "2.28.2"
        testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
        testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

        testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = "2.3.0")
        testImplementation(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
        testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "schema-registry-mock-junit5", version = "2.3.0") {
            exclude(group = "junit")
        }
        testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
            exclude(group = "org.projectlombok")
        }

        testImplementation("com.ginsberg:junit5-system-exit:1.0.0")
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
