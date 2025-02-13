plugins {
    id("com.bakdata.release") version "1.6.1"
    id("com.bakdata.sonar") version "1.6.1"
    id("com.bakdata.sonatype") version "1.6.1"
    id("io.freefair.lombok") version "8.11"
}

allprojects {
    group = "com.bakdata.kafka"

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
        maven(url = "https://jitpack.io")
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
            name.set("Ramin Gharib")
            id.set("raminqaf")
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

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "java-test-fixtures")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(11)
        }
    }
}
