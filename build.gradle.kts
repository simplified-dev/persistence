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
    api("com.github.simplified-dev:collections") { version { strictly("6586657") } }
    api("com.github.simplified-dev:utils") { version { strictly("ca4cbca") } }
    api("com.github.simplified-dev:reflection") { version { strictly("746e607") } }
    api("com.github.simplified-dev:gson-extras") { version { strictly("35d2257") } }
    api("com.github.simplified-dev:scheduler") { version { strictly("695f985") } }

    // JetBrains Annotations
    api(libs.annotations)

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

    // Optional JCache provider - required at runtime only when a consumer selects JpaCacheProvider.HAZELCAST_*.
    // compileOnly keeps Hazelcast off the published runtime classpath; testRuntimeOnly makes the
    // parallel JpaCacheHazelcastTest suite functional without forcing it on consumers.
    compileOnly(libs.hazelcast)
    testRuntimeOnly(libs.hazelcast)
}

tasks.test {
    useJUnitPlatform()
}
