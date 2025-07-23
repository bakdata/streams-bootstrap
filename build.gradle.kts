plugins {
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok)
}

allprojects {
    group = "com.bakdata.kafka"

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }

    tasks.register<Javadoc>("aggregateJavadoc") {
        group = "documentation"
        description = "Generates aggregated Javadoc from all modules"

        val allJavaSources = files()
        val allClasspaths = files()

        val moduleNames = listOf(
            ":streams-bootstrap-core",
            ":streams-bootstrap-test",
            ":streams-bootstrap-large-messages",
            ":streams-bootstrap-cli",
            ":streams-bootstrap-cli-test"
        )

        moduleNames.map { project(it) }.forEach { sub ->
            sub.plugins.withId("java") {
                val sourceSets = sub.extensions.getByType<SourceSetContainer>()
                val main = sourceSets.getByName("main")
                allJavaSources.from(main.allJava)
                allClasspaths.from(main.compileClasspath)
            }
        }

        source = allJavaSources.asFileTree
        classpath = allClasspaths

        setDestinationDir(layout.buildDirectory.dir("aggregateJavadoc").get().asFile)
    }

    tasks.register<Jar>("aggregateJavadocJar") {
        dependsOn("aggregateJavadoc")
        group = "documentation"
        description = "Packages the aggregated Javadoc into a JAR"

        archiveClassifier.set("javadoc")
        archiveBaseName.set("aggregated")
        archiveVersion.set("")

        from(tasks.named<Javadoc>("aggregateJavadoc").map { it.destinationDir!! })

        destinationDirectory.set(layout.buildDirectory.dir("libs"))
    }
}

subprojects {
    plugins.matching { it is JavaPlugin }.all {
        apply(plugin = "java-test-fixtures")
        apply(plugin = "io.freefair.lombok")

        configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(17)
            }
        }
    }

    publication {
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
}
