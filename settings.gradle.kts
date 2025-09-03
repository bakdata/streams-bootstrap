pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "streams-bootstrap"

include(
        ":streams-bootstrap-core",
        ":streams-bootstrap-test",
        ":streams-bootstrap-large-messages",
        ":streams-bootstrap-cli",
        ":streams-bootstrap-cli-test",
        ":streams-bootstrap-bom",
)
