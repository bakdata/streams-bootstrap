description = "Base classes to create standalone Java applications using picocli"

plugins {
    id("java-library")
}

dependencies {
    api(project(":streams-bootstrap-core"))
    api(libs.picocli)
    implementation(libs.slf4j)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(testFixtures(project(":streams-bootstrap-test")))
    testImplementation(project(":streams-bootstrap-cli-test"))
    testImplementation(libs.junit.systemExit)
    testImplementation(libs.log4j.slf4j2)
}

tasks.withType<Test> {
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-javaagent:${
                configurations.testRuntimeClasspath.get().files.find {
                    it.name.contains("junit5-system-exit")
                }
            }"
        )
    })
}
