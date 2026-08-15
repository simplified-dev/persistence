package dev.simplified.persistence.model;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.persistence.CacheExpiry;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Cacheable;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

import java.util.concurrent.TimeUnit;

@Entity
@Cacheable
@Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
@Table(name = "test_child")
@CacheExpiry(value = 2, length = TimeUnit.SECONDS)
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class TestChildModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    @ManyToOne
    @JoinColumn(name = "parent_id")
    private TestParentModel parent;

    @Column(name = "value")
    private String value;

}
