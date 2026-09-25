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
    api("com.github.simplified-dev:collections") { version { strictly("4029e80") } }
    api("com.github.simplified-dev:utils") { version { strictly("92ae878") } }
    api("com.github.simplified-dev:reflection") { version { strictly("5186e88") } }
    api("com.github.simplified-dev:gson-extras") { version { strictly("3ac0d4f") } }
    api("com.github.simplified-dev:scheduler") { version { strictly("6903600") } }

    // JetBrains Annotations
    api(libs.annotations)

    // Logging
    api(libs.log4j2.api)
    implementation(libs.log4j2.core)

    // Simplified Annotations
    compileOnly(libs.simplified.annotations)
    annotationProcessor(libs.simplified.annotations)
    testCompileOnly(libs.simplified.annotations)
    testAnnotationProcessor(libs.simplified.annotations)

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
