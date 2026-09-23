package dev.simplified.persistence;

import dev.simplified.annotations.AccessLevel;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.driver.MariaDbDriver;
import dev.simplified.persistence.source.RelationalOrigin;
import dev.simplified.persistence.source.Source;
import dev.simplified.reflection.Reflection;
import dev.simplified.util.Logging;
import dev.simplified.util.SystemUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * Immutable configuration class for JPA sessions, constructed via its nested {@link Builder}.
 *
 * <p>Three things: the types a session registers and where their rows come from, through the
 * {@link RepositoryFactory} and its {@link Source}; the database that authors the rows no factory
 * names an origin for, through a {@link RelationalOrigin}; and the parser and log level everything
 * runs under.
 *
 * <p>The database is optional and it is the gate. Present, {@link JpaSession} opens it and the whole
 * relational stack exists - service registry, metadata, session factory, JCache regions. Absent, none
 * of that runs, none of it needs to be on the classpath, and every registered type reads through the
 * {@link Source} its factory declares.
 *
 * <p>Use {@link #commonSql()} for a MariaDB read from the environment, {@link #common(RelationalOrigin)}
 * for a pre-filled {@link Builder}, or {@link #builder()} for full control.
 *
 * @see Builder
 * @see JpaSession
 * @see RelationalOrigin
 * @see RepositoryFactory
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class JpaConfig {

    private final @NotNull UUID uniqueId = UUID.randomUUID();

    /**
     * The database this session opens, empty when it has none.
     */
    private final @NotNull Optional<RelationalOrigin> database;

    private final @NotNull GsonSettings gsonSettings;
    private final @NotNull RepositoryFactory repositoryFactory;
    private @NotNull Logging.Level logLevel = Logging.Level.WARN;

    /**
     * Checks whether the current log level includes the given level.
     *
     * @param level the log level to compare against
     * @return {@code true} if the current log level encompasses {@code level}
     */
    public boolean isLogLevel(@NotNull Logging.Level level) {
        return this.logLevel.includes(level);
    }

    /**
     * Returns a new {@link Builder} for constructing a {@link JpaConfig} instance.
     *
     * @return a new builder with default values
     */
    public static @NotNull Builder builder() {
        return new Builder();
    }

    /**
     * Returns a pre-filled {@link Builder} over the given database.
     *
     * @param database the database the session opens
     * @return a pre-filled builder
     */
    public static @NotNull Builder common(@NotNull RelationalOrigin database) {
        return builder()
            .withDatabase(database)
            .withLogLevel(Logging.Level.WARN);
    }

    /**
     * Builds a MariaDB configuration from the {@code DATABASE_HOST}, {@code DATABASE_PORT},
     * {@code DATABASE_SCHEMA}, {@code DATABASE_USER} and {@code DATABASE_PASSWORD} environment
     * variables.
     *
     * @return a fully constructed MariaDB configuration
     * @throws IllegalStateException if any of the connection variables is unset
     */
    public static @NotNull JpaConfig commonSql() {
        return common(
            MariaDbDriver.at(
                    required("DATABASE_HOST"),
                    Integer.parseInt(required("DATABASE_PORT")),
                    required("DATABASE_SCHEMA")
                )
                .as(required("DATABASE_USER"), required("DATABASE_PASSWORD"))
                .build()
        ).build();
    }

    /**
     * Reads a connection variable that has to be set.
     *
     * @param variable the environment variable name
     * @return its value
     * @throws IllegalStateException if the variable is unset or blank
     */
    private static @NotNull String required(@NotNull String variable) {
        return SystemUtil.getEnv(variable)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException(String.format("'%s' holds no value to connect with", variable)));
    }

    /**
     * Sets the log level and propagates it to all underlying loggers.
     *
     * <p>Affected loggers include JBoss Logging and Logback always, and - with a database - every
     * logger that database brings with it.
     *
     * @param logLevel the new log level to apply
     */
    public void setLogLevel(@NotNull Logging.Level logLevel) {
        this.logLevel = logLevel;
        Logging.setLevel("org.jboss.logging", logLevel);
        Logging.setLevel("ch.qos.logback", logLevel);
        this.database.ifPresent(origin -> origin.applyLogLevel(logLevel, this.getRepositoryFactory().getModels()));
    }

    /**
     * Fluent builder for constructing {@link JpaConfig} instances.
     *
     * <p>Everything carries a default. A database is described where it lives - on
     * {@link RelationalOrigin} - so there is nothing here to validate: an origin that exists is one
     * that can be reached.
     *
     * @see JpaConfig#builder()
     */
    public static class Builder {

        private @Nullable RelationalOrigin database;
        private @NotNull GsonSettings gsonSettings = GsonSettings.defaults();
        private @Nullable RepositoryFactory repositoryFactory;
        private @NotNull Logging.Level logLevel = Logging.Level.WARN;

        /**
         * Sets the database a session opens, which is what makes it relational.
         */
        public @NotNull Builder withDatabase(@NotNull RelationalOrigin database) {
            this.database = database;
            return this;
        }

        /**
         * Sets the {@link GsonSettings} used for JSON serialization within the session.
         */
        public @NotNull Builder withGsonSettings(@NotNull GsonSettings gsonSettings) {
            this.gsonSettings = gsonSettings;
            return this;
        }

        /**
         * Sets the factory naming the types a session registers and where their rows come from.
         */
        public @NotNull Builder withRepositoryFactory(@NotNull RepositoryFactory repositoryFactory) {
            this.repositoryFactory = repositoryFactory;
            return this;
        }

        /**
         * Sets the log level applied at {@link #build()} time.
         */
        public @NotNull Builder withLogLevel(@NotNull Logging.Level level) {
            this.logLevel = level;
            return this;
        }

        /**
         * Validates builder flags, resolves defaults, and constructs the {@link JpaConfig}.
         *
         * @return a fully constructed, immutable {@link JpaConfig}
         */
        public @NotNull JpaConfig build() {
            Reflection.validateFlags(this);

            JpaConfig jpaConfig = new JpaConfig(
                Optional.ofNullable(this.database),
                this.gsonSettings,
                this.repositoryFactory != null ? this.repositoryFactory : RepositoryFactory.of(JpaModel.class)
            );

            jpaConfig.setLogLevel(this.logLevel);
            return jpaConfig;
        }

    }

}
