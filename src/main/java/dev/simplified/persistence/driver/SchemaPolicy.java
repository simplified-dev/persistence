package dev.simplified.persistence.driver;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Who owns the schema a driver connects to.
 *
 * <p>Two questions ride on this and they are not the same question. Whether any DDL is emitted, and
 * whether column widths are ours to choose - a schema someone else maintains decides its own widths,
 * and one generated here has no reason to inherit the 255 a mapping defaults to when what it holds is
 * unpredictable.
 */
@RequiredArgsConstructor
public enum SchemaPolicy {

    /**
     * The schema exists and is maintained elsewhere. No DDL is emitted and the widths are the
     * database's.
     */
    EXTERNAL(null),

    /**
     * The schema is created on connect and dropped on close, so it lasts exactly as long as the
     * session does.
     */
    CREATE_DROP("create-drop"),

    /**
     * The schema is created if absent and widened to match the mapping, and it outlives the session.
     */
    UPDATE("update");

    /**
     * The {@code hibernate.hbm2ddl.auto} value this policy asks for, or {@code null} to set none.
     */
    @Getter private final @Nullable String hbm2ddl;

    /**
     * Whether the schema is generated from the mapping rather than read as it stands.
     *
     * @return {@code true} when the column widths are this library's to choose
     */
    public boolean isGenerated() {
        return this.hbm2ddl != null;
    }

    /**
     * The policy's name, for a message naming what a session connected under.
     */
    @Override
    public @NotNull String toString() {
        return this.name().toLowerCase().replace('_', '-');
    }

}
