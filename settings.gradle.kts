pluginManagement {
    repositories {
        gradlePluginPortal()
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
