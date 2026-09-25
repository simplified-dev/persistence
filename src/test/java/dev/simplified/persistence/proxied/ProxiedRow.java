package dev.simplified.persistence.proxied;

import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

/**
 * A row whose parent is a row of its own type, fetched lazily - so a read that reaches a row before
 * its parent has Hibernate stand a proxy in for the parent, and the parent's own row comes back as
 * that proxy.
 *
 * <p>A session refuses to register it for that lazy association, so only a database maps it. It sits
 * apart from the other mapped fixtures because a scan anchored on those would otherwise map it too.
 */
@Entity
@Getter
@Setter
@Table(name = "proxied")
public class ProxiedRow implements JpaModel {

    /**
     * The row's identifier.
     */
    @Id
    @Column(name = "id")
    private Long id;

    /**
     * The row this one hangs under, loaded only when first read.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "parent_id")
    private ProxiedRow parent;

}
