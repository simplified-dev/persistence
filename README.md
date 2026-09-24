# Persistence

JPA/Hibernate ORM abstraction layer with L2 caching (EhCache), custom Gson-backed Hibernate types, and a repository pattern implementation. Provides repositories that hold each model's rows in memory, session management, per-type hydration cadences, sources that read rows from a relational database or from layered JSON documents, and support for multiple database drivers.

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

- **Repository pattern** - `Repository` holds one generation of a model's rows in memory; every `Sortable` finder answers from it without I/O, and a property declaring `@Indexed` is answered by a hash probe
- **Sessions** - `JpaSession` hydrates every type a `JpaConfig` registers from its one `Source`, resolves links before it publishes a generation, and rebuilds a written type together with every type that links into it
- **Session management** - `SessionManager` registers a session once it has hydrated, looks repositories up and routes writes across every session it holds, and shuts them down together
- **Hydration cadence** - `@Hydration` declares how often a type is checked against its source in the background and when its generation reports stale; a due type whose source fingerprint has not moved is not read, one that moved is rebuilt with every type linking into it, and a source that fingerprints nothing rebuilds every due type. A type declaring none has no cadence of its own, and is rebuilt when it or a type it links into is written through its session, or when a type it links into comes due on its own cadence and has moved
- **Links** - `@Linked` fills a field with the row, or rows, its id property names, and keeps that field out of serialization
- **Sources** - One `Source` contract for where a type's rows come from: `RelationalSource` over a database, `DocumentSource` over the layered JSON documents a `DocumentOrigin` names, and `Source.Writable` - `RelationalSource` and `DocumentSource.Writable` - for a source that also takes writes
- **L2 caching** - EhCache-backed second-level cache for an open database, held in a cache manager no other database shares, with one TTL for every mapped type and configurable cache concurrency strategies
- **Custom Hibernate types** - `GsonValueType` with a codec per field shape (annotated class, `List<E>`, `Map<K, V>`, `Optional<I>`) for JSON columns
- **Multiple database drivers** - MariaDB, H2 (file, memory, TCP), Oracle Thin, PostgreSQL, SQL Server
- **Type converters** - Built-in auto-applied JPA attribute converter for `UUID`

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

Define a model:

```java
import dev.simplified.persistence.Hydration;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.*;

import java.util.concurrent.TimeUnit;

@Entity
@Table(name = "users")
@Hydration(every = 5, unit = TimeUnit.MINUTES)
public class User implements JpaModel {

    @Id
    private Long id;
    private String name;
}
```

Open a database and connect a session over it. The caller opens the database and hands it to the session as the source every registered type is read from:

```java
ConcurrentList<Class<JpaModel>> models = JpaModel.resolveModels(User.class);
RelationalSource database = MariaDbDriver.at("localhost", "mydb")
    .as("root", "secret")
    .open(models, GsonSettings.defaults().create(), Logging.Level.WARN);

SessionManager sessionManager = new SessionManager();
sessionManager.connect(new JpaConfig(models, database));
```

The list `open` maps and the list a `JpaConfig` registers are separate: a type registered with the session holds a generation in memory, while a mapped type left out of it is reached through the database's own Hibernate access.

Query the held rows, and write through the session so the generation follows the write:

```java
Repository<User> users = sessionManager.getRepository(User.class);
ConcurrentList<User> all = users.findAll();

sessionManager.write(WriteRequest.upsert(User.class, List.of(user)));
```

Reach Hibernate through the database, and shut down in order:

```java
database.transaction(session -> {
    session.persist(archived);
});

sessionManager.shutdown();
database.close();
```

A write that goes straight to Hibernate like this bypasses the session, so a type the session registers keeps the rows it held until its next rebuild; write a registered type through the session instead.

Shutting down is optional. A `SessionManager` holding a session and a `RelationalSource` still open each register a JVM shutdown hook, which shuts the sessions down and closes the database at exit; shutting down explicitly releases them earlier and removes the hooks. Sessions go first, because a session reading a closed database fails its next write, rebuild or tick. The JVM runs shutdown hooks concurrently, so a rebuild or tick still running at exit can fail against a database that is closing.

A session over layered JSON documents opens nothing and closes nothing - the source is built and handed in:

```java
sessionManager.connect(new JpaConfig(
    JpaModel.resolveModels(Item.class),
    new DocumentSource(origin, GsonSettings.defaults().create())
));
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
| `dev.simplified.persistence` | Core interfaces and classes (`Repository`, `JpaRepository`, `JpaSession`, `SessionManager`, `JpaConfig`, `JpaModel`, `@Hydration`, `@Linked`) |
| `dev.simplified.persistence.converter` | JPA attribute converters (`UUIDConverter`) |
| `dev.simplified.persistence.driver` | Database driver abstraction with implementations for MariaDB, H2, Oracle, PostgreSQL, SQL Server |
| `dev.simplified.persistence.exception` | `JpaException` for persistence-related errors |
| `dev.simplified.persistence.source` | Where a type's rows come from and how they go back (`Source`, `DocumentSource`, `RelationalSource`, `DocumentOrigin`, `WriteRequest`) |
| `dev.simplified.persistence.type` | Gson-backed custom Hibernate types (`GsonValueType`, `GsonType`) with type and converter registrars |

### Project Structure

```
persistence/
├── src/
│   ├── main/java/dev/simplified/persistence/
│   │   ├── CacheMissingStrategy.java
│   │   ├── Hydration.java
│   │   ├── HydrationState.java
│   │   ├── JpaConfig.java
│   │   ├── JpaExclusionStrategy.java
│   │   ├── JpaGsonContributor.java
│   │   ├── JpaModel.java
│   │   ├── JpaRepository.java
│   │   ├── JpaSession.java
│   │   ├── Linked.java
│   │   ├── Repository.java
│   │   ├── SessionManager.java
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
│   │   ├── source/
│   │   │   ├── DocumentOrigin.java
│   │   │   ├── DocumentSource.java
│   │   │   ├── RelationalSource.java
│   │   │   ├── Source.java
│   │   │   └── WriteRequest.java
│   │   └── type/
│   │       ├── ConverterRegistrar.java
│   │       ├── GsonType.java
│   │       ├── GsonValueType.java
│   │       └── TypeRegistrar.java
│   ├── main/resources/META-INF/services/
│   │   └── dev.simplified.gson.GsonContributor
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
