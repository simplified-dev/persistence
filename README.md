# Persistence

JPA/Hibernate ORM abstraction layer with L2 caching (EhCache or Hazelcast), custom Gson-backed Hibernate types, and a repository pattern implementation. Provides read-only cached repositories, session management, per-entity TTL annotations, JSON- or SQL-backed entity stores, and support for multiple database drivers.

> [!IMPORTANT]
> This library is under active development. APIs may change between releases until a stable `1.0.0` is published.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Supported Drivers](#supported-drivers)
- [Architecture](#architecture)
  - [Package Overview](#package-overview)
  - [Project Structure](#project-structure)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Repository pattern** - Read-only cached `Repository` interface with `JpaRepository` implementation for CRUD operations, cache eviction, and stream-based querying
- **Session management** - `SessionManager` registry for multiple concurrent `JpaSession` instances with cross-session repository lookup, reconnection, and coordinated shutdown
- **L2 caching** - EhCache- or Hazelcast-backed second-level cache with per-entity TTL via `@CacheExpiry` annotation and configurable cache concurrency strategies
- **Custom Hibernate types** - `GsonValueType` with a codec per field shape (annotated class, `List<E>`, `Map<K, V>`, `Optional<I>`) for JSON columns
- **Multiple database drivers** - MariaDB, H2 (file, memory, TCP), Oracle Thin, PostgreSQL, SQL Server
- **Type converters** - Built-in auto-applied JPA attribute converter for `UUID`
- **Entity stores** - One `EntityStore` contract for where a type's rows come from, expressible as a lambda; a `null` store leaves the type to the database
- **Repository factory** - `RepositoryFactory` with topological entity sorting, per-type store registration, and classpath-based model discovery
- **Foreign ID resolution** - `@ForeignIds` transient field population for cross-entity relationships loaded from non-relational sources
- **Stale entity cleanup** - Automatic removal of database rows not present in the latest store load, in FK-safe reverse topological order
- **External asset tracking** - `ExternalAssetState` and `ExternalAssetEntryState` record per-source and per-entry content hashes so a poller can tell what actually changed

## Getting Started

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| [Java](https://adoptium.net/) | **21+** | Required (LTS recommended) |
| [Gradle](https://gradle.org/) | **9.4+** | Or use the included `gradlew` wrapper |
| [Git](https://git-scm.com/) | 2.x+ | For cloning the repository |

### Installation

Published via [JitPack](https://jitpack.io/#simplified-dev/persistence). Add the JitPack repository and dependency to your build file.

<details>
<summary>Gradle (Kotlin DSL)</summary>

```kotlin
repositories {
    mavenCentral()
    maven(url = "https://jitpack.io")
}

dependencies {
    implementation("com.github.simplified-dev:persistence:master-SNAPSHOT")
}
```

</details>

<details>
<summary>Gradle (Groovy DSL)</summary>

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.simplified-dev:persistence:master-SNAPSHOT'
}
```

</details>

> [!TIP]
> Replace `master-SNAPSHOT` with a specific commit hash or tag for reproducible builds.

## Usage

Define a JPA entity model:

```java
import dev.simplified.persistence.CacheExpiry;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.*;

import java.util.concurrent.TimeUnit;

@Entity
@CacheExpiry(value = 5, length = TimeUnit.MINUTES)
public class User implements JpaModel {

    @Id
    private Long id;
    private String name;
}
```

Configure and connect a session:

```java
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.driver.MariaDbDriver;

JpaConfig config = JpaConfig.common(new MariaDbDriver(), "mydb")
    .withHost("localhost")
    .withPort(3306)
    .withUser("root")
    .withPassword("secret")
    .build();

SessionManager sessionManager = new SessionManager();
sessionManager.connect(config);
```

Query cached data through the repository:

```java
import dev.simplified.persistence.Repository;

Repository<User> userRepo = sessionManager.getRepository(User.class);
ConcurrentList<User> users = userRepo.findAll();
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
| `dev.simplified.persistence` | Core interfaces and classes (`Repository`, `JpaRepository`, `JpaSession`, `SessionManager`, `RepositoryFactory`, `JpaConfig`, `JpaModel`, `@CacheExpiry`, `@ForeignIds`) |
| `dev.simplified.persistence.asset` | Change-detection state for external asset origins (`ExternalAssetState`, `ExternalAssetEntryState`) |
| `dev.simplified.persistence.converter` | JPA attribute converters (`UUIDConverter`) |
| `dev.simplified.persistence.driver` | Database driver abstraction with implementations for MariaDB, H2, Oracle, PostgreSQL, SQL Server |
| `dev.simplified.persistence.exception` | `JpaException` for persistence-related errors |
| `dev.simplified.persistence.store` | Where a type's rows come from (`EntityStore`, `FileFetcher`, `ManifestIndex`, `WriteRequest`) |
| `dev.simplified.persistence.type` | Gson-backed custom Hibernate types (`GsonValueType`, `GsonType`) with type and converter registrars |

### Project Structure

```
persistence/
├── src/
│   ├── main/java/dev/simplified/persistence/
│   │   ├── CacheExpiry.java
│   │   ├── CacheMissingStrategy.java
│   │   ├── ForeignIds.java
│   │   ├── JpaCacheProvider.java
│   │   ├── JpaConfig.java
│   │   ├── JpaExclusionStrategy.java
│   │   ├── JpaModel.java
│   │   ├── JpaRepository.java
│   │   ├── JpaSession.java
│   │   ├── Repository.java
│   │   ├── RepositoryFactory.java
│   │   ├── SessionManager.java
│   │   ├── asset/
│   │   │   ├── ExternalAssetEntryState.java
│   │   │   ├── ExternalAssetState.java
│   │   │   └── package-info.java
│   │   ├── converter/
│   │   │   └── UUIDConverter.java
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
│   │   ├── store/
│   │   │   ├── EntityStore.java
│   │   │   ├── FileFetcher.java
│   │   │   ├── ManifestIndex.java
│   │   │   └── WriteRequest.java
│   │   └── type/
│   │       ├── ConverterRegistrar.java
│   │       ├── GsonType.java
│   │       ├── GsonValueType.java
│   │       └── TypeRegistrar.java
│   └── test/
├── build.gradle.kts
├── gradle/
│   └── libs.versions.toml
└── LICENSE.md
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
| [Hazelcast](https://hazelcast.com/) | 5.6.0 | Compile-only (test runtime); required only for a `HAZELCAST_*` cache provider |
| [Log4j2](https://logging.apache.org/log4j/) | 2.25.3 | API (log level configuration and `@Log4j2` logging) |
| [JetBrains Annotations](https://github.com/JetBrains/java-annotations) | 26.0.2 | API |
| [Simplified Annotations](https://github.com/Simplified-Dev/annotations) | 2.6.1 | Compile-only |
| [JUnit 5](https://junit.org/junit5/) | 5.11.4 | Test |
| [Hamcrest](http://hamcrest.org/) | 2.2 | Test |
| [collections](https://github.com/Simplified-Dev/collections) | pinned commit | API (Simplified-Dev) |
| [utils](https://github.com/Simplified-Dev/utils) | pinned commit | API (Simplified-Dev) |
| [reflection](https://github.com/Simplified-Dev/reflection) | pinned commit | API (Simplified-Dev) |
| [gson-extras](https://github.com/Simplified-Dev/gson-extras) | pinned commit | API (Simplified-Dev) |
| [scheduler](https://github.com/Simplified-Dev/scheduler) | pinned commit | API (Simplified-Dev) |

> [!NOTE]
> The Simplified-Dev dependencies are pinned to exact JitPack commits rather than to a moving branch. See [`build.gradle.kts`](build.gradle.kts) for the current hashes.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style guidelines, and how to submit a pull request.

## License

This project is licensed under the **Apache License 2.0** - see [LICENSE.md](LICENSE.md) for the full text.
