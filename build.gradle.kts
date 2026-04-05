plugins {
    id("java-library")
}

group = "dev.simplified"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    // Simplified Libraries
    api("com.github.simplified-dev:collections:master-SNAPSHOT")
    api("com.github.simplified-dev:utils:master-SNAPSHOT")
    api("com.github.simplified-dev:reflection:master-SNAPSHOT")
    api("com.github.simplified-dev:gson-extras:master-SNAPSHOT")
    api("com.github.simplified-dev:scheduler:master-SNAPSHOT")

    // JetBrains Annotations
    api(libs.annotations)

    // Simplified Annotations
    api(libs.simplified.annotations)
    annotationProcessor(libs.simplified.annotations)

    // Logging
    api(libs.log4j2.api)
    implementation(libs.log4j2.core)

    // Lombok Annotations
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    testCompileOnly(libs.lombok)
    testAnnotationProcessor(libs.lombok)

    // Tests
    testImplementation(libs.hamcrest)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
    testImplementation(libs.junit.platform.launcher)

    // Serialization
    api(libs.gson)

    // Database
    api(libs.hibernate.core)
    implementation(libs.hibernate.hikaricp)
    implementation(libs.hibernate.jcache)
    implementation(libs.mariadb)
    implementation(libs.h2)
    implementation(libs.ehcache)
}

tasks.test {
    useJUnitPlatform()
}
