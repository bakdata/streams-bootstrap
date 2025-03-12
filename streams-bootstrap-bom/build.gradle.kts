description = "BOM for streams-bootstrap."

plugins {
    id("java-platform")
}

dependencies {
    constraints {
        api(project(":streams-bootstrap-core"))
        api(project(":streams-bootstrap-cli"))
        api(project(":streams-bootstrap-large-messages"))
        api(project(":streams-bootstrap-test"))
    }
}
