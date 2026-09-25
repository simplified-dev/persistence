package dev.simplified.persistence.model;

import dev.simplified.annotations.EqualsAndHashCode;
import dev.simplified.annotations.Getter;
import dev.simplified.annotations.Setter;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.type.GsonType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.List;
import java.util.Optional;

/**
 * An entity carrying one column of every generic field shape the Gson user types resolve, sized
 * to the shapes the skyblock models actually declare: lists of a concrete element, lists whose
 * element is itself an interface, maps keyed by a string and by an enum, maps whose value type
 * cannot be resolved to a concrete class, an annotated scalar, and optionals over each of the
 * eight wrapper types that carry a native SQL type plus the ones that do not.
 *
 * <p>Every field is nullable and none is initialised, so a row can be written with a null in any
 * column to pin what the write and the read do with it.
 */
@Entity
@Table(name = "gson_fixture")
@Getter
@Setter
@EqualsAndHashCode(of = "id", identity = EqualsAndHashCode.Identity.INSTANCE_OF)
public class GsonFixtureModel implements JpaModel {

    @Id
    @Column(name = "id")
    private int id;

    /**
     * The common case - a list of a concrete element class.
     */
    @Column(name = "strings")
    private ConcurrentList<String> strings;

    /**
     * The numeric list, which comes back as {@code Integer} only because the read targets an array.
     */
    @Column(name = "integers")
    private ConcurrentList<Integer> integers;

    /**
     * A list of an annotated class, so a list codec stacked over a scalar one.
     */
    @Column(name = "substitutes")
    private ConcurrentList<Substitute> substitutes;

    /**
     * A list whose element type is an interface, which no element-typed instance can be registered for.
     */
    @Column(name = "payload_list")
    private ConcurrentList<ConcurrentMap<String, Object>> payloadList;

    /**
     * A list of an entity, the one shape whose registrar does carry a guard against it.
     */
    @Column(name = "entity_list")
    private ConcurrentList<TestParentModel> entityList;

    /**
     * The common map - string keys, boxed doubles.
     */
    @Column(name = "doubles")
    private ConcurrentMap<String, Double> doubles;

    /**
     * A map of boxed integers, which the map codec resolves without the array indirection a list needs.
     */
    @Column(name = "counts")
    private ConcurrentMap<String, Integer> counts;

    /**
     * A map of open values, where every JSON number reads back as a {@code Double}.
     */
    @Column(name = "payloads")
    private ConcurrentMap<String, Object> payloads;

    /**
     * A map keyed by an enum.
     */
    @Column(name = "by_rarity")
    private ConcurrentMap<Rarity, Double> byRarity;

    /**
     * A map whose value type is an interface, which no key/value-typed instance can be registered for.
     */
    @Column(name = "series")
    private ConcurrentMap<String, List<Double>> series;

    /**
     * An annotated class held directly, so the scalar codec on its own.
     */
    @Column(name = "payload")
    private Substitute payload;

    @Column(name = "opt_string")
    private Optional<String> optString = Optional.empty();

    @Column(name = "opt_boolean")
    private Optional<Boolean> optBoolean = Optional.empty();

    @Column(name = "opt_integer")
    private Optional<Integer> optInteger = Optional.empty();

    @Column(name = "opt_long")
    private Optional<Long> optLong = Optional.empty();

    @Column(name = "opt_short")
    private Optional<Short> optShort = Optional.empty();

    @Column(name = "opt_byte")
    private Optional<Byte> optByte = Optional.empty();

    /**
     * An optional over an annotated class, which has no native SQL type and so goes through Gson.
     */
    @Column(name = "opt_payload")
    private Optional<Substitute> optPayload = Optional.empty();

    /**
     * An optional over an enum, which likewise has no native SQL type.
     */
    @Column(name = "opt_rarity")
    private Optional<Rarity> optRarity = Optional.empty();

    /**
     * An optional over an entity, the one shape whose registrar carries no guard against it.
     */
    @Column(name = "opt_entity")
    private Optional<TestParentModel> optEntity = Optional.empty();

    /**
     * A value class stored as JSON, shaped after the reference-plus-amounts rows the skyblock
     * models hold lists of.
     */
    @Getter
    @Setter
    @GsonType
    @EqualsAndHashCode
    public static class Substitute {

        /**
         * Id of the thing being referenced.
         */
        private String id = "";

        /**
         * Decimal places to render the amount to.
         */
        private int precision = 0;

        /**
         * The tier this substitute applies at.
         */
        @Enumerated(EnumType.STRING)
        private Rarity rarity = Rarity.COMMON;

        /**
         * The amount granted, keyed by the level that grants it.
         */
        private ConcurrentMap<Integer, Double> values = Concurrent.newMap();

        /**
         * Constructs an empty substitute, which is the constructor Gson and Hibernate use.
         */
        public Substitute() {
        }

        /**
         * Constructs a substitute with every component set.
         *
         * @param id id of the thing being referenced
         * @param precision decimal places to render the amount to
         * @param rarity the tier this substitute applies at
         * @param values the amount granted, keyed by the level that grants it
         */
        public Substitute(String id, int precision, Rarity rarity, ConcurrentMap<Integer, Double> values) {
            this.id = id;
            this.precision = precision;
            this.rarity = rarity;
            this.values = values;
        }

    }

    /**
     * A tier, standing in for the enum the skyblock maps are keyed by.
     */
    public enum Rarity {

        COMMON,
        RARE,
        LEGENDARY

    }

}
