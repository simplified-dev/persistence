# Persistence

JPA/Hibernate ORM abstraction layer with L2 caching (EhCache), custom Gson-backed Hibernate type converters, and a repository pattern implementation. Provides read-only cached repositories, session management, per-entity TTL annotations, and support for multiple database drivers.

> [!IMPORTANT]
> This library is under active development. APIs may change between releases until a stable `1.0.0` is published.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Gradle (Kotlin DSL)](#gradle-kotlin-dsl)
  - [Gradle (Groovy DSL)](#gradle-groovy-dsl)
- [Usage](#usage)
- [Supported Drivers](#supported-drivers)
- [Architecture](#architecture)
  - [Package Overview](#package-overview)
  - [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Repository pattern** - Read-only cached `Repository` interface with `JpaRepository` implementation
- **Session management** - `SessionManager` and `JpaSession` for Hibernate session lifecycle
- **L2 caching** - EhCache-backed second-level cache with per-entity TTL via `@CacheExpiry` annotation
- **Custom Hibernate types** - Gson-backed type converters for JSON columns (`GsonJsonType`, `GsonListType`, `GsonMapType`, `GsonOptionalType`)
- **Multiple database drivers** - MariaDB, H2 (file, memory, TCP), Oracle Thin, PostgreSQL, SQL Server
- **Type converters** - Built-in converters for `Color`, `UUID`, and Unicode strings
- **JSON persistence sources** - `Source` interface with `JsonSource` implementation for file-based data
- **Repository factory** - `RepositoryFactory` for creating and managing repository instances

## Getting Started

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| [Java](https://adoptium.net/) | **21+** | Required (LTS recommended) |
| [Gradle](https://gradle.org/) | **9.4+** | Or use the included `gradlew` wrapper |
| [Git](https://git-scm.com/) | 2.x+ | For cloning the repository |

### Installation

Published via [JitPack](https://jitpack.io/#simplified-dev/persistence). Add the JitPack repository and dependency to your build file.

### Gradle (Kotlin DSL)

```kotlin
repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation("com.github.simplified-dev:persistence:master-SNAPSHOT")
}
```

### Gradle (Groovy DSL)

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.simplified-dev:persistence:master-SNAPSHOT'
}
```

> [!TIP]
> Replace `master-SNAPSHOT` with a specific commit hash or tag for reproducible builds.

## Usage

Define a JPA entity model:

```java
import dev.simplified.persistence.CacheExpiry;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.*;

@Entity
@CacheExpiry(duration = 5, unit = TimeUnit.MINUTES)
public class User extends JpaModel {

    @Id
    private Long id;
    private String name;
}
```

Create a repository and query cached data:

```java
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.JpaConfig;

// Configure and open a session
JpaConfig config = JpaConfig.builder()
    .driver(new MariaDbDriver("localhost", 3306, "mydb"))
    .username("root")
    .password("secret")
    .build();

SessionManager sessionManager = new SessionManager(config);
JpaRepository<User, Long> userRepo = new JpaRepository<>(sessionManager, User.class);

// Cached read-only access
User user = userRepo.findById(1L);
```

## Supported Drivers

| Driver | Class | Connection |
|--------|-------|------------|
| MariaDB | `MariaDbDriver` | `jdbc:mariadb://host:port/database` |
| H2 File | `H2FileDriver` | `jdbc:h2:file:path` |
| H2 Memory | `H2MemoryDriver` | `jdbc:h2:mem:name` |
| H2 TCP | `H2TcpDriver` | `jdbc:h2:tcp://host:port/database` |
| Oracle Thin | `OracleThinDriver` | `jdbc:oracle:thin:@host:port:sid` |
| PostgreSQL | `PostgreSqlDriver` | `jdbc:postgresql://host:port/database` |
| SQL Server | `SqlServerDriver` | `jdbc:sqlserver://host:port;databaseName=db` |

> [!NOTE]
> Only MariaDB and H2 drivers are included as runtime dependencies. Oracle, PostgreSQL, and SQL Server drivers must be added to your project separately.

## Architecture

### Package Overview

| Package | Description |
|---------|-------------|
| `dev.simplified.persistence` | Core interfaces and classes (`Repository`, `JpaRepository`, `JpaSession`, `SessionManager`, `RepositoryFactory`, `JpaConfig`, `JpaModel`, `@CacheExpiry`) |
| `dev.simplified.persistence.converter` | Hibernate attribute converters (`ColorConverter`, `UUIDConverter`, `UnicodeConverter`) |
| `dev.simplified.persistence.driver` | Database driver abstraction with implementations for MariaDB, H2, Oracle, PostgreSQL, SQL Server |
| `dev.simplified.persistence.exception` | `JpaException` for persistence-related errors |
| `dev.simplified.persistence.source` | Data source interfaces and JSON file-based implementation |
| `dev.simplified.persistence.type` | Gson-backed custom Hibernate types (`GsonJsonType`, `GsonListType`, `GsonMapType`, `GsonOptionalType`, `GsonType`) with type and converter registrars |

### Project Structure

```
persistence/
├── src/
│   ├── main/java/dev/simplified/persistence/
│   │   ├── CacheExpiry.java
│   │   ├── CacheMissingStrategy.java
│   │   ├── ForeignIds.java
│   │   ├── JpaConfig.java
│   │   ├── JpaExclusionStrategy.java
│   │   ├── JpaModel.java
│   │   ├── JpaRepository.java
│   │   ├── JpaSession.java
│   │   ├── Repository.java
│   │   ├── RepositoryFactory.java
│   │   ├── SessionManager.java
│   │   ├── converter/
│   │   │   ├── ColorConverter.java
│   │   │   ├── UUIDConverter.java
│   │   │   └── UnicodeConverter.java
│   │   ├── driver/
│   │   │   ├── H2FileDriver.java
│   │   │   ├── H2MemoryDriver.java
│   │   │   ├── H2TcpDriver.java
│   │   │   ├── JpaDriver.java
│   │   │   ├── MariaDbDriver.java
│   │   │   ├── OracleThinDriver.java
│   │   │   ├── PostgreSqlDriver.java
│   │   │   └── SqlServerDriver.java
│   │   ├── exception/
│   │   │   └── JpaException.java
│   │   ├── source/
│   │   │   ├── JsonSource.java
│   │   │   └── Source.java
│   │   └── type/
│   │       ├── ConverterRegistrar.java
│   │       ├── GsonJsonType.java
│   │       ├── GsonListType.java
│   │       ├── GsonMapType.java
│   │       ├── GsonOptionalType.java
│   │       ├── GsonType.java
│   │       └── TypeRegistrar.java
│   └── test/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle/
│   └── libs.versions.toml
├── LICENSE.md
└── lombok.config
```

## Dependencies

| Dependency | Version | Scope |
|------------|---------|-------|
| [Hibernate Core](https://hibernate.org/orm/) | 7.3.0.Final | API |
| [Hibernate HikariCP](https://hibernate.org/orm/) | 7.3.0.Final | Implementation |
| [Hibernate JCache](https://hibernate.org/orm/) | 7.3.0.Final | Implementation |
| [Gson](https://github.com/google/gson) | 2.11.0 | API |
| [MariaDB Connector/J](https://mariadb.com/kb/en/mariadb-connector-j/) | 3.5.3 | Implementation |
| [H2 Database](https://h2database.com/) | 2.3.232 | Implementation |
| [EhCache](https://www.ehcache.org/) | 3.10.8 | Implementation |
| [Log4j2](https://logging.apache.org/log4j/) | 2.25.3 | API |
| [JetBrains Annotations](https://github.com/JetBrains/java-annotations) | 26.0.2 | API |
| [Simplified Annotations](https://github.com/SkyBlock-Simplified/Annotation-Processor) | 1.0.4 | API |
| [Lombok](https://projectlombok.org/) | 1.18.36 | Compile-only |
| [JUnit 5](https://junit.org/junit5/) | 5.11.4 | Test |
| [Hamcrest](http://hamcrest.org/) | 2.2 | Test |
| [collections](https://github.com/Simplified-Dev/collections) | master-SNAPSHOT | API (Simplified-Dev) |
| [utils](https://github.com/Simplified-Dev/utils) | master-SNAPSHOT | API (Simplified-Dev) |
| [reflection](https://github.com/Simplified-Dev/reflection) | master-SNAPSHOT | API (Simplified-Dev) |
| [gson-extras](https://github.com/Simplified-Dev/gson-extras) | master-SNAPSHOT | API (Simplified-Dev) |
| [scheduler](https://github.com/Simplified-Dev/scheduler) | master-SNAPSHOT | API (Simplified-Dev) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style guidelines, and how to submit a pull request.

## License

This project is licensed under the **Apache License 2.0** - see [LICENSE.md](LICENSE.md) for the full text.
