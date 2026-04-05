package dev.simplified.persistence.model;

import dev.simplified.persistence.CacheExpiry;
import dev.simplified.persistence.JpaModel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Entity
@Table(name = "test_parent")
@CacheExpiry(value = 2, length = TimeUnit.SECONDS)
@Getter
@Setter
public class TestParentModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    @Column(name = "name")
    private String name;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TestParentModel that)) return false;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

}
