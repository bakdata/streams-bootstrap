pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

rootProject.name = "streams-bootstrap"

include(
        ":streams-bootstrap-core",
        ":streams-bootstrap-test",
        ":streams-bootstrap-large-messages",
        ":streams-bootstrap-cli",
        ":streams-bootstrap-bom",
)
