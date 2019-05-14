description = "Collection of commonly used modules when writing a Kafka Streams Application"


plugins {
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.1.4"
    id("com.bakdata.sonatype") version "1.1.4"
    id("org.hildan.github.changelog") version "0.8.0"
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
        "testImplementation"("org.junit.jupiter:junit-jupiter-api:5.3.0")
        "testRuntimeOnly"("org.junit.jupiter:junit-jupiter-engine:5.3.0")
        "testImplementation"(group = "org.assertj", name = "assertj-core", version = "3.11.1")
        "testImplementation"(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests", version = "1.0.1")
        "testImplementation"(group = "org.apache.kafka", name = "kafka-streams-test-utils", version = "2.2.0")

        "api"(group = "info.picocli", name = "picocli", version = "2.3.0")
        val kafkaVersion: String by project
        "api"(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
        val confluentVersion: String by project
        "implementation"(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
        "implementation"(group = "log4j", name = "log4j", version = "1.2.17")

        "compileOnly"("org.projectlombok:lombok:1.18.6")
        "annotationProcessor"("org.projectlombok:lombok:1.18.6")
        "testCompileOnly"("org.projectlombok:lombok:1.18.6")
        "testAnnotationProcessor"("org.projectlombok:lombok:1.18.6")
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}
