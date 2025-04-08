import nl.littlerobots.vcu.plugin.resolver.VersionSelectors

plugins {
    alias(libs.plugins.release)
    alias(libs.plugins.sonar)
    alias(libs.plugins.sonatype)
    alias(libs.plugins.lombok)
    id("nl.littlerobots.version-catalog-update") version "1.0.0"
}

versionCatalogUpdate {
    sortByKey.set(false)
    versionSelector(VersionSelectors.STABLE)
}

allprojects {
    group = "com.bakdata.kafka"

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

subprojects {
    plugins.matching { it is JavaPlugin }.all {
        apply(plugin = "java-test-fixtures")
        apply(plugin = "io.freefair.lombok")

        configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(11)
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
