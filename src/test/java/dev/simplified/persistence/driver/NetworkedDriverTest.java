package dev.simplified.persistence.driver;

import dev.simplified.collection.Concurrent;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.Connection;
import dev.simplified.persistence.source.RelationalSource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The driver class and the address each networked driver names on its default port, read back from
 * the refusal a database gives before it connects when its JDBC driver is not on the classpath - which
 * neither of these is here.
 */
class NetworkedDriverTest {

    private static final @NotNull Connection.Credentials ACCOUNT = new Connection.Credentials("user", "password");

    private static @NotNull String refusal(@NotNull RelationalSource.Builder database) {
        return assertThrows(
            JpaException.class,
            () -> database.withModels(Concurrent.newUnmodifiableList()).build()
        ).getMessage();
    }

    @Test
    @DisplayName("SQL Server names the current Microsoft driver, its url form and port 1433")
    void sqlServerDefaults() {
        assertThat(
            refusal(SqlServerDriver.at("db.example", "shop", ACCOUNT)),
            equalTo("No JDBC driver 'com.microsoft.sqlserver.jdbc.SQLServerDriver' on the classpath for 'jdbc:sqlserver://db.example:1433;databaseName=shop (external)'")
        );
    }

    @Test
    @DisplayName("Oracle names the Thin driver and port 1521")
    void oracleDefaults() {
        assertThat(
            refusal(OracleThinDriver.at("db.example", "ORCL", ACCOUNT)),
            equalTo("No JDBC driver 'oracle.jdbc.driver.OracleDriver' on the classpath for 'jdbc:oracle:thin:@db.example:1521:ORCL (external)'")
        );
    }

}
