# Persistence

JPA/Hibernate ORM abstraction with L2 caching (EhCache), Gson-backed Hibernate types, and repository pattern.

## Package Structure

- `dev.simplified.persistence` - `Repository`, `JpaRepository`, `JpaSession`, `SessionManager`, `RepositoryFactory`, `JpaConfig`, `JpaModel`, `@CacheExpiry`, `CacheMissingStrategy`, `ForeignIds`, `JpaExclusionStrategy`
- `dev.simplified.persistence.converter` - `ColorConverter`, `UUIDConverter`, `UnicodeConverter`
- `dev.simplified.persistence.driver` - `JpaDriver`, `MariaDbDriver`, `H2FileDriver`, `H2MemoryDriver`, `H2TcpDriver`, `OracleThinDriver`, `PostgreSqlDriver`, `SqlServerDriver`
- `dev.simplified.persistence.exception` - `JpaException`
- `dev.simplified.persistence.source` - `Source`, `JsonSource`
- `dev.simplified.persistence.type` - `GsonJsonType`, `GsonListType`, `GsonMapType`, `GsonOptionalType`, `GsonType`, `ConverterRegistrar`, `TypeRegistrar`

## Key Classes

- `Repository` - read-only cached repository interface
- `JpaRepository` - full JPA repository implementation
- `SessionManager` - Hibernate session lifecycle management
- `JpaConfig` - database connection configuration builder
- `JpaDriver` - abstract driver; subclasses for MariaDB, H2, Oracle, PostgreSQL, SQL Server
- `GsonJsonType` / `GsonListType` / `GsonMapType` - custom Hibernate types backed by Gson
- `@CacheExpiry` - per-entity TTL annotation for L2 cache

## Dependencies

- Simplified-Dev: `collections`, `utils`, `reflection`, `gson-extras`, `scheduler`
- External: Hibernate 7.3.0.Final, Gson 2.11.0, MariaDB 3.5.3, H2 2.3.232, EhCache 3.10.8, Log4j2 2.25.3, Lombok, JetBrains Annotations, Simplified Annotations 1.0.4
- Test: JUnit 5.11.4, Hamcrest 2.2

## Build

```bash
./gradlew build
./gradlew test
```

## Config

- Java 21
- Gradle 9.4.1 (wrapper)
- Group: `dev.simplified`, artifact: `persistence`, version: `1.0.0`
- 3 test files
