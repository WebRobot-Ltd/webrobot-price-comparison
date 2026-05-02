plugins {
    id("scala")
    id("java-library")
}

group   = "eu.webrobot"
version = versions().projectV

dependencies {
    compileOnly(project(":webrobot-plugin-sdk"))
    compileOnly("org.scala-lang:scala-library:${versions().scalaV}")
    compileOnly("org.slf4j:slf4j-api:1.7.36")
}

tasks.withType<Jar> {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
