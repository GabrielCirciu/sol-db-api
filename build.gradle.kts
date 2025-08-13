plugins {
    kotlin("jvm") version "2.2.0"
    id("com.gradleup.shadow") version "9.0.0-rc1"
    id("java")
}

group = "com.github.binkibonk"
version = "0.1.2"
description = "Sol DB Core for PostgreSQL integration"

repositories {
    mavenCentral()
    maven("https://repo.papermc.io/repository/maven-public/")
    maven("https://repo.clojars.org/")
    mavenLocal()
}

dependencies {
    compileOnly("io.papermc.paper:paper-api:1.20.1-R0.1-SNAPSHOT")
    implementation("com.github.puregero:multilib:1.2.4")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.postgresql:postgresql:42.7.1")
}


val targetJavaVersion = 21
java {
    val javaVersion = JavaVersion.toVersion(targetJavaVersion)
    sourceCompatibility = javaVersion
    targetCompatibility = javaVersion
    if (JavaVersion.current() < javaVersion) {
        toolchain.languageVersion = JavaLanguageVersion.of(targetJavaVersion)
    }
}

kotlin {
    jvmToolchain(targetJavaVersion)
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    if (targetJavaVersion >= 10 || JavaVersion.current().isJava10Compatible) {
        options.release.set(targetJavaVersion)
    }
}

tasks.processResources {
    val props = mapOf("version" to version, "description" to description)
    inputs.properties(props)
    filteringCharset = "UTF-8"
    filesMatching("plugin.yml") {
        expand(props)
    }
}

tasks.shadowJar {
    archiveBaseName.set(project.name)
    archiveClassifier.set("")
    relocate("com.github.puregero.multilib", "com.binkibonk.soldbapi.libs.multilib")
    relocate("com.zaxxer.hikari", "com.binkibonk.soldbapi.libs.hikari")
    relocate("org.postgresql", "com.binkibonk.soldbapi.libs.postgresql")
    relocate("kotlinx.coroutines", "com.binkibonk.soldbapi.libs.kotlinx.coroutines")
    relocate("kotlin", "com.binkibonk.soldbapi.libs.kotlin")
}

tasks.build {
    dependsOn(tasks.shadowJar)
} 

tasks.compileKotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.fromTarget(targetJavaVersion.toString()))
    }
}