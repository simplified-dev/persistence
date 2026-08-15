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
    api("com.github.simplified-dev:collections") { version { strictly("23f01b6") } }
    api("com.github.simplified-dev:utils") { version { strictly("381e317") } }
    api("com.github.simplified-dev:reflection") { version { strictly("d02f3ea") } }
    api("com.github.simplified-dev:gson-extras") { version { strictly("c4bde8d") } }
    api("com.github.simplified-dev:scheduler") { version { strictly("f486253") } }

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

    // Optional JCache provider - required at runtime only when a consumer selects JpaCacheProvider.HAZELCAST_*.
    // compileOnly keeps Hazelcast off the published runtime classpath; testRuntimeOnly makes the
    // parallel JpaCacheHazelcastTest suite functional without forcing it on consumers.
    compileOnly(libs.hazelcast)
    testRuntimeOnly(libs.hazelcast)
}

tasks.test {
    useJUnitPlatform()
}
