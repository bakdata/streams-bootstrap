description = "Collection of commonly used modules when writing a Kafka Streams Application"


plugins {
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.1.4"
    id("com.bakdata.sonatype") version "1.1.4"
    id("org.hildan.github.changelog") version "0.8.0"
    id("com.commercehub.gradle.plugin.avro") version "0.16.0"
}

allprojects {
    group = "com.bakdata.${rootProject.name}"

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
            name.set("Arvid Heise")
            id.set("AHeise")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

allprojects {
    apply(plugin = "java-library")

    configure<JavaPluginConvention> {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    dependencies {
        val kafkaVersion: String by project
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.4.0")
        "testImplementation"("org.junit.jupiter:junit-jupiter-params:5.4.0")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.4.0")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.13.2")
        "testImplementation"(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = "2.2.0")
        "testImplementation"(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = kafkaVersion)
        "testImplementation"(group = "com.bakdata.fluent-kafka-streams-tests", name = "schema-registry-mock-junit5", version = "2.2.0") {
            exclude(group = "junit")
        }
        "testImplementation"(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
            exclude(group = "org.projectlombok")
        }
        "testImplementation"("com.ginsberg:junit5-system-exit:1.0.0")
        "implementation"(group = "org.apache.kafka", name = "kafka_2.13", version = kafkaVersion)

        "api"(group = "info.picocli", name = "picocli", version = "4.0.4")
        "api"(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        val confluentVersion: String by project
        "implementation"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
        "implementation"(group = "log4j", name = "log4j", version = "1.2.17")
        "implementation"(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.25")
        "implementation"(group = "com.google.guava", name = "guava", version = "29.0-jre")
        val avroVersion: String by project
        "api"(group = "org.apache.avro", name = "avro", version = avroVersion)
        "api"(group = "org.jooq", name = "jool", version = "0.9.14")
        "api"(group = "org.apache.commons", name = "commons-lang3", version = "3.9")

        "compileOnly"("org.projectlombok:lombok:1.18.6")
        "annotationProcessor"("org.projectlombok:lombok:1.18.6")
        "testCompileOnly"("org.projectlombok:lombok:1.18.6")
        "testAnnotationProcessor"("org.projectlombok:lombok:1.18.6")

        "testImplementation"(group = "org.mockito", name = "mockito-core", version = "2.28.2")
        "testImplementation"(group = "org.mockito", name = "mockito-junit-jupiter", version = "2.28.2")
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
