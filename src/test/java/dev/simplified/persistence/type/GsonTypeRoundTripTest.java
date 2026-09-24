package dev.simplified.persistence.type;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.floating.FloatingOptionalModel;
import dev.simplified.persistence.model.GsonFixtureModel.Rarity;
import dev.simplified.persistence.model.GsonFixtureModel.Substitute;
import dev.simplified.persistence.model.GsonFixtureModel;
import dev.simplified.persistence.model.TestParentModel;
import dev.simplified.persistence.source.RelationalSource;
import dev.simplified.util.Logging;
import org.hibernate.Session;
import org.hibernate.mapping.BasicValue;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.type.BasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.CustomType;
import org.hibernate.usertype.UserType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Characterisation of the Gson-backed {@link UserType} implementations, written against the types
 * Hibernate actually binds to a property rather than against the classes that supply them, so it
 * survives any rearrangement of those classes.
 *
 * <p>Everything here is a statement about what the module does today. The shapes are the ones the
 * skyblock models declare, in the proportions they declare them, because those are the ones a
 * change has to keep working.
 */
@Tag("fast")
class GsonTypeRoundTripTest {

    /**
     * The database the fixtures are written to and read back from. Nothing registers the fixture
     * types with a session - they are mapped and reached through Hibernate, which is all this needs.
     */
    private static RelationalSource database;

    @BeforeAll
    static void open() {
        database = H2MemoryDriver.named("gson_type_round_trip")
            .isUsing2ndLevelCache(false)
            .isUsingQueryCache(false)
            .withDefaultCacheExpiryMs(0)
            .open(JpaModel.resolveModels(GsonFixtureModel.class), GsonSettings.defaults().create(), Logging.Level.WARN);
    }

    @AfterAll
    static void close() {
        if (database != null)
            database.close();
    }

    /**
     * What a row holds after it has been through the database, for one column of every shape.
     */
    @Nested
    class Values {

        @Test
        void listOfStringsKeepsItsElements() {
            GsonFixtureModel loaded = roundTrip(10, row -> row.setStrings(Concurrent.newList("alpha", "beta")));

            assertEquals(List.of("alpha", "beta"), loaded.getStrings());
            assertInstanceOf(String.class, loaded.getStrings().getFirst());
        }

        @Test
        void listOfIntegersComesBackAsIntegerNotDouble() {
            GsonFixtureModel loaded = roundTrip(11, row -> row.setIntegers(Concurrent.newList(1, 2, 3)));

            // The read targets an Integer[] rather than a List<Integer> for exactly this reason - a
            // list read as a List<Integer> hands back Doubles and every numeric list is silently wrong.
            assertEquals(List.of(1, 2, 3), loaded.getIntegers());
            assertInstanceOf(Integer.class, elementOf(loaded.getIntegers()));
        }

        @Test
        void listOfAnnotatedClassesKeepsEveryComponent() {
            Substitute substitute = new Substitute("crit_damage", 2, Rarity.RARE, values(3, 12.5));
            GsonFixtureModel loaded = roundTrip(12, row -> row.setSubstitutes(Concurrent.newList(substitute)));

            assertEquals(1, loaded.getSubstitutes().size());
            assertEquals(substitute, loaded.getSubstitutes().getFirst());
        }

        @Test
        void mapOfDoublesKeepsItsValues() {
            GsonFixtureModel loaded = roundTrip(13, row -> row.setDoubles(map("health", 2.5)));

            assertEquals(Map.of("health", 2.5), loaded.getDoubles());
        }

        @Test
        void mapOfIntegersComesBackAsIntegerWithoutTheArrayIndirection() {
            // A map is read through a parameterized TypeToken, so unlike a list it needs no array
            // detour to keep a boxed integer boxed as an integer.
            GsonFixtureModel loaded = roundTrip(14, row -> row.setCounts(map("slots", 7)));

            assertEquals(Map.of("slots", 7), loaded.getCounts());
            assertInstanceOf(Integer.class, valueOf(loaded.getCounts(), "slots"));
        }

        @Test
        void mapOfOpenValuesTurnsEveryNumberIntoADouble() {
            GsonFixtureModel loaded = roundTrip(15, row -> row.setPayloads(map("amount", 9)));

            assertInstanceOf(Double.class, valueOf(loaded.getPayloads(), "amount"));
            assertEquals(9.0, valueOf(loaded.getPayloads(), "amount"));
        }

        @Test
        void mapKeyedByAnEnumKeepsItsKeys() {
            GsonFixtureModel loaded = roundTrip(16, row -> row.setByRarity(map(Rarity.LEGENDARY, 2.5)));

            assertEquals(Map.of(Rarity.LEGENDARY, 2.5), loaded.getByRarity());
            assertInstanceOf(Rarity.class, loaded.getByRarity().keySet().iterator().next());
        }

        @Test
        void annotatedClassHeldDirectlyKeepsEveryComponent() {
            Substitute substitute = new Substitute("strength", 1, Rarity.COMMON, values(1, 5.0));
            GsonFixtureModel loaded = roundTrip(17, row -> row.setPayload(substitute));

            assertEquals(substitute, loaded.getPayload());
        }

        @Test
        void optionalOfStringIsPresent() {
            GsonFixtureModel loaded = roundTrip(18, row -> row.setOptString(Optional.of("text")));

            assertEquals(Optional.of("text"), loaded.getOptString());
        }

        @Test
        void optionalOfEachNativeWrapperKeepsItsBox() {
            GsonFixtureModel loaded = roundTrip(19, row -> {
                row.setOptBoolean(Optional.of(true));
                row.setOptInteger(Optional.of(42));
                row.setOptLong(Optional.of(43L));
                row.setOptShort(Optional.of((short) 44));
                row.setOptByte(Optional.of((byte) 45));
            });

            assertEquals(Optional.of(true), loaded.getOptBoolean());
            assertEquals(Optional.of(42), loaded.getOptInteger());
            assertEquals(Optional.of(43L), loaded.getOptLong());
            assertEquals(Optional.of((short) 44), loaded.getOptShort());
            assertEquals(Optional.of((byte) 45), loaded.getOptByte());
            assertInstanceOf(Integer.class, loaded.getOptInteger().orElseThrow());
            assertInstanceOf(Short.class, loaded.getOptShort().orElseThrow());
            assertInstanceOf(Byte.class, loaded.getOptByte().orElseThrow());
        }

        @Test
        void optionalOfANonNativeInnerTypeGoesThroughGson() {
            Substitute substitute = new Substitute("defense", 3, Rarity.RARE, values(2, 1.5));
            GsonFixtureModel loaded = roundTrip(20, row -> {
                row.setOptPayload(Optional.of(substitute));
                row.setOptRarity(Optional.of(Rarity.LEGENDARY));
            });

            assertEquals(Optional.of(substitute), loaded.getOptPayload());
            assertEquals(Optional.of(Rarity.LEGENDARY), loaded.getOptRarity());
        }

    }

    /**
     * The two shapes no element-typed instance can be registered for, because the element or the
     * value is itself an interface. Neither reaches Gson at all - Hibernate binds them to its own
     * Java-serialisation handling of the container interface, and the evidence is that they keep
     * runtime detail no JSON document could carry.
     */
    @Nested
    class InterfaceParameters {

        @Test
        void listOfMapsIsJavaSerialised() {
            ConcurrentMap<String, Object> element = map("amount", 5);
            GsonFixtureModel loaded = roundTrip(30, row -> row.setPayloadList(Concurrent.newList(element)));

            Object first = elementOf(loaded.getPayloadList());
            assertInstanceOf(ConcurrentMap.class, first, "a Gson read of an Object[] could not produce a ConcurrentMap");
            assertInstanceOf(Integer.class, ((Map<?, ?>) first).get("amount"), "a Gson read would have made this a Double");
        }

        @Test
        void mapOfListsIsJavaSerialised() {
            GsonFixtureModel loaded = roundTrip(31, row -> row.setSeries(map("curve", List.of(1.0, 2.0))));

            Object series = valueOf(loaded.getSeries(), "curve");
            assertEquals(List.of(1.0, 2.0), series);
            assertEquals(List.of(1.0, 2.0).getClass(), series.getClass(), "the exact List.of implementation survived, so no JSON was involved");
        }

    }

    /**
     * What a null on either side of the boundary becomes. Entity code reads these columns without
     * a null check, so a list that came back null would be a null pointer at the call site.
     */
    @Nested
    class Nulls {

        @Test
        void nullListWritesAnEmptyJsonArrayRatherThanSqlNull() {
            persist(40, row -> {});

            assertEquals("[]", rawColumn("strings", 40));
            assertEquals("{}", rawColumn("payloads", 40));
        }

        @Test
        void nullAnnotatedScalarWritesSqlNull() {
            persist(41, row -> {});

            assertNull(rawColumn("payload", 41));
        }

        @Test
        void sqlNullReadsBackAsAnEmptyCollection() {
            persist(42, row -> row.setStrings(Concurrent.newList("gone")));
            execute("UPDATE \"gson_fixture\" SET \"strings\" = NULL, \"payloads\" = NULL WHERE \"id\" = 42");

            GsonFixtureModel loaded = load(42);
            assertNotNull(loaded.getStrings());
            assertTrue(loaded.getStrings().isEmpty());
            assertInstanceOf(ConcurrentList.class, loaded.getStrings());
            assertNotNull(loaded.getPayloads());
            assertTrue(loaded.getPayloads().isEmpty());
            assertInstanceOf(ConcurrentMap.class, loaded.getPayloads());
        }

        @Test
        void emptyStringNormalisesToAnAbsentOptional() {
            persist(43, row -> row.setOptString(Optional.of("present")));
            execute("UPDATE \"gson_fixture\" SET \"opt_string\" = '' WHERE \"id\" = 43");

            assertEquals(Optional.empty(), load(43).getOptString());
        }

        @Test
        void emptyOptionalWritesSqlNull() {
            persist(44, row -> {});

            assertNull(rawColumn("opt_string", 44));
            assertNull(rawColumn("opt_integer", 44));
        }

        @Test
        void deepCopyOfNullAnswersTheEmptyValueOfEachShape() {
            // Hibernate copies a field's value before a persist and does not skip a null one, so a
            // field left uninitialised arrives here. Each shape answers with its own empty.
            assertEquals(Concurrent.newList(), boundType("strings").deepCopy(null));
            assertEquals(Concurrent.newMap(), boundType("payloads").deepCopy(null));
            assertEquals(Optional.empty(), boundType("optString").deepCopy(null));
            assertNull(boundType("payload").deepCopy(null));
        }

        @Test
        void anUninitialisedOptionalFieldPersistsAsSqlNull() {
            GsonFixtureModel row = new GsonFixtureModel();
            row.setId(45);
            row.setOptString(null);

            assertDoesNotThrow(() -> database.transaction(s -> { s.persist(row); }));
            assertNull(rawColumn("opt_string", 45));
            assertEquals(Optional.empty(), load(45).getOptString());
        }

    }

    /**
     * The second-level cache path, which never touches a {@link ResultSet} - a cached value is
     * disassembled to something serialisable and rebuilt from it.
     */
    @Nested
    class CachePath {

        @Test
        void listDisassemblesToJsonAndRebuildsThroughTheArray() {
            UserType<Object> type = boundType("integers");

            assertEquals("[1,2]", type.disassemble(Concurrent.newList(1, 2)));
            assertEquals(List.of(1, 2), type.assemble("[1,2]", null));
            assertInstanceOf(Integer.class, elementOf((List<?>) type.assemble("[1,2]", null)));
        }

        @Test
        void nullListDisassemblesToAnEmptyJsonArray() {
            UserType<Object> type = boundType("strings");

            assertEquals("[]", type.disassemble(null));
            assertEquals(Concurrent.newList(), type.assemble(null, null));
        }

        @Test
        void nullMapDisassemblesToAnEmptyJsonObject() {
            UserType<Object> type = boundType("payloads");

            assertEquals("{}", type.disassemble(null));
            assertEquals(Concurrent.newMap(), type.assemble(null, null));
        }

        @Test
        void optionalOfANativeInnerTypeDisassemblesToTheBoxItselfNotToJson() {
            // The cached form of a native optional is the wrapper, not a string. Round-tripping it
            // through a string instead would hand a String back to a field typed Optional<Integer>.
            UserType<Object> type = boundType("optInteger");

            assertEquals(7, type.disassemble(Optional.of(7)));
            assertEquals(Optional.of(7), type.assemble(7, null));
            assertNull(type.disassemble(Optional.empty()));
            assertEquals(Optional.empty(), type.assemble(null, null));
        }

        @Test
        void optionalOfANonNativeInnerTypeDisassemblesToJson() {
            UserType<Object> type = boundType("optRarity");

            assertEquals("\"LEGENDARY\"", type.disassemble(Optional.of(Rarity.LEGENDARY)));
            assertEquals(Optional.of(Rarity.LEGENDARY), type.assemble("\"LEGENDARY\"", null));
        }

        @Test
        void annotatedScalarDisassemblesNullToNull() {
            assertNull(boundType("payload").disassemble(null));
            assertNull(boundType("payload").assemble(null, null));
        }

    }

    /**
     * The registration dance: a container type registers one instance per parameterisation under a
     * composite key plus a raw fallback under the container's own name, and then re-points each
     * property at its own instance once the metadata is built. Both halves are observable, because
     * the fallback and the per-parameter instance disagree about what a number is.
     */
    @Nested
    class Registration {

        @Test
        void rawFallbacksAreRegisteredUnderTheContainerNames() {
            assertNotNull(registered(ConcurrentList.class.getName()));
            assertNotNull(registered(ConcurrentMap.class.getName()));
            assertNotNull(registered(Optional.class.getName()));
        }

        @Test
        void theRawListFallbackIsObjectTyped() {
            assertInstanceOf(Double.class, elementOf((List<?>) registered(ConcurrentList.class.getName()).assemble("[1]", null)));
        }

        @Test
        void theRawMapFallbackIsObjectTyped() {
            assertInstanceOf(Double.class, valueOf((Map<?, ?>) registered(ConcurrentMap.class.getName()).assemble("{\"a\":1}", null), "a"));
        }

        @Test
        void theRawOptionalFallbackIsStringTyped() {
            assertEquals(Types.VARCHAR, registered(Optional.class.getName()).getSqlType());
        }

        @Test
        void everyContainerPropertyIsUpgradedToItsOwnInstance() {
            assertSame(registered("GsonList:java.lang.String"), boundType("strings"));
            assertSame(registered("GsonList:java.lang.Integer"), boundType("integers"));
            assertSame(registered("GsonList:" + Substitute.class.getName()), boundType("substitutes"));
            assertSame(registered("GsonMap:java.lang.String:java.lang.Double"), boundType("doubles"));
            assertSame(registered("GsonMap:java.lang.String:java.lang.Integer"), boundType("counts"));
            assertSame(registered("GsonMap:java.lang.String:java.lang.Object"), boundType("payloads"));
            assertSame(registered("GsonMap:" + Rarity.class.getName() + ":java.lang.Double"), boundType("byRarity"));
            assertSame(registered("GsonOptional:java.lang.String"), boundType("optString"));
            assertSame(registered("GsonOptional:java.lang.Integer"), boundType("optInteger"));
        }

        @Test
        void anAnnotatedClassIsRegisteredUnderItsOwnName() {
            assertSame(registered(Substitute.class.getName()), boundType("payload"));
        }

        @Test
        void aPropertyWithAnInterfaceParameterIsNotBoundToAnyGsonType() {
            assertFalse(isUserType("payloadList"));
            assertFalse(isUserType("series"));
        }

    }

    /**
     * The two type codes a column carries, which are not the same number. The binding is what a
     * read and a write go through; the column is what the schema was created as, and it is fixed
     * before any property is re-pointed - so an optional binds as INTEGER over a VARCHAR column.
     */
    @Nested
    class SqlTypes {

        @Test
        void containersAndAnnotatedScalarsBindAsClob() {
            assertEquals(Types.CLOB, boundSqlType("strings"));
            assertEquals(Types.CLOB, boundSqlType("integers"));
            assertEquals(Types.CLOB, boundSqlType("substitutes"));
            assertEquals(Types.CLOB, boundSqlType("doubles"));
            assertEquals(Types.CLOB, boundSqlType("counts"));
            assertEquals(Types.CLOB, boundSqlType("payloads"));
            assertEquals(Types.CLOB, boundSqlType("byRarity"));
            assertEquals(Types.CLOB, boundSqlType("payload"));
        }

        @Test
        void eachNativeInnerTypeBindsAsItsOwnSqlType() {
            assertEquals(Types.VARCHAR, boundSqlType("optString"));
            assertEquals(Types.BOOLEAN, boundSqlType("optBoolean"));
            assertEquals(Types.INTEGER, boundSqlType("optInteger"));
            assertEquals(Types.BIGINT, boundSqlType("optLong"));
            assertEquals(Types.SMALLINT, boundSqlType("optShort"));
            assertEquals(Types.TINYINT, boundSqlType("optByte"));
        }

        @Test
        void eachNonNativeInnerTypeBindsAsClob() {
            assertEquals(Types.CLOB, boundSqlType("optPayload"));
            assertEquals(Types.CLOB, boundSqlType("optRarity"));
            assertEquals(Types.CLOB, boundSqlType("optEntity"));
        }

        @Test
        void theCreatedSchemaFollowsTheFallbackRatherThanTheBinding() {
            Map<String, Integer> columns = columnTypes();

            // A container column is neither CLOB nor anything Gson chose - the schema was created
            // from Hibernate's own Serializable handling of the container interface.
            assertEquals(Types.VARBINARY, columns.get("strings"));
            assertEquals(Types.VARBINARY, columns.get("integers"));
            assertEquals(Types.VARBINARY, columns.get("substitutes"));
            assertEquals(Types.VARBINARY, columns.get("doubles"));
            assertEquals(Types.VARBINARY, columns.get("payloads"));
            assertEquals(Types.VARBINARY, columns.get("by_rarity"));
            assertEquals(Types.VARBINARY, columns.get("payload_list"));
            assertEquals(Types.VARBINARY, columns.get("series"));

            // Every optional column is the VARCHAR of the string-inner fallback, whatever the
            // property was later re-pointed at.
            assertEquals(Types.VARCHAR, columns.get("opt_string"));
            assertEquals(Types.VARCHAR, columns.get("opt_boolean"));
            assertEquals(Types.VARCHAR, columns.get("opt_integer"));
            assertEquals(Types.VARCHAR, columns.get("opt_long"));
            assertEquals(Types.VARCHAR, columns.get("opt_short"));
            assertEquals(Types.VARCHAR, columns.get("opt_byte"));
            assertEquals(Types.VARCHAR, columns.get("opt_payload"));
            assertEquals(Types.VARCHAR, columns.get("opt_rarity"));
            assertEquals(Types.VARCHAR, columns.get("opt_entity"));

            // An annotated class is registered under its own name before the metadata is built, so
            // it is the one shape whose column matches what the type asks for.
            assertEquals(Types.CLOB, columns.get("payload"));
        }

    }

    /**
     * The two container shapes that can name an entity as their argument, which do not agree about
     * it. The list registrar refuses one and leaves the field to Hibernate; the optional registrar
     * has no such guard and stores a JSON copy of the row rather than a foreign key to it. Neither
     * column is an association, and neither is one a change may quietly turn into one.
     */
    @Nested
    class EntityArguments {

        @Test
        void aListOfEntitiesIsLeftToHibernate() {
            assertFalse(isUserType("entityList"), "the list shape refuses an element that is an entity");
            assertEquals(Types.VARBINARY, columnTypes().get("entity_list"));
        }

        @Test
        void anOptionalOfAnEntityHoldsAJsonCopy() {
            TestParentModel parent = new TestParentModel();
            parent.setId(77);
            parent.setName("held");

            GsonFixtureModel loaded = roundTrip(50, row -> row.setOptEntity(Optional.of(parent)));

            assertEquals("{\"id\":77,\"name\":\"held\"}", rawColumn("opt_entity", 50));
            assertTrue(loaded.getOptEntity().isPresent());
            assertEquals(77, loaded.getOptEntity().orElseThrow().getId());
            assertEquals("held", loaded.getOptEntity().orElseThrow().getName());
        }

        @Test
        void theColumnCarriesNoForeignKey() {
            assertTrue(entity().getProperty("optEntity").getValue() instanceof BasicValue);
        }

    }

    /**
     * The two wrapper types the optional path claims to handle natively but cannot create a schema
     * for. Their SQL type carries no length, and the column that would hold them is emitted with
     * the dialect's length placeholder left in it, so the table is never created and every query
     * against the type fails.
     */
    @Nested
    class UnschemableOptionals {

        @Test
        void doubleAndFloatInnerTypesFailToCreateTheirTable() {
            SessionManager manager = new SessionManager();
            ConcurrentList<Class<JpaModel>> models = JpaModel.resolveModels(FloatingOptionalModel.class);
            RelationalSource floating = H2MemoryDriver.named("floating_optional")
                .isUsing2ndLevelCache(false)
                .isUsingQueryCache(false)
                .withDefaultCacheExpiryMs(0)
                .open(models, GsonSettings.defaults().create(), Logging.Level.WARN);

            try {
                // Schema export logs the failed CREATE and carries on, so the table is absent. The
                // hydration pass then reads every registered type, which is where the absence
                // surfaces: connecting fails rather than mounting a session whose first query on
                // this type would have.
                assertThrows(JpaException.class, () -> manager.connect(new JpaConfig(models, floating)));
            } finally {
                manager.shutdown();
                floating.close();
            }
        }

    }

    private static @NotNull PersistentClass entity() {
        return database.getMetadata().getEntityBinding(GsonFixtureModel.class.getName());
    }

    private static @NotNull BasicValue basicValue(@NotNull String property) {
        return (BasicValue) entity().getProperty(property).getValue();
    }

    private static boolean isUserType(@NotNull String property) {
        return basicValue(property).getResolution().getLegacyResolvedBasicType() instanceof CustomType<?>;
    }

    @SuppressWarnings("unchecked")
    private static @NotNull UserType<Object> asUserType(@NotNull BasicType<?> type) {
        return (UserType<Object>) assertInstanceOf(CustomType.class, type).getUserType();
    }

    private static @NotNull UserType<Object> boundType(@NotNull String property) {
        return asUserType(basicValue(property).getResolution().getLegacyResolvedBasicType());
    }

    private static int boundSqlType(@NotNull String property) {
        return boundType(property).getSqlType();
    }

    private static @NotNull BasicTypeRegistry registry() {
        return basicValue("integers").getTypeConfiguration().getBasicTypeRegistry();
    }

    private static @NotNull UserType<Object> registered(@NotNull String key) {
        BasicType<?> type = registry().getRegisteredType(key);
        assertNotNull(type, "no type registered under '" + key + "'");
        return asUserType(type);
    }

    private static @NotNull Map<String, Integer> columnTypes() {
        Map<String, Integer> types = new LinkedHashMap<>();

        database.with(s -> {
            s.doWork(connection -> {
                try (
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT * FROM \"gson_fixture\"")
                ) {
                    ResultSetMetaData metaData = resultSet.getMetaData();

                    for (int index = 1; index <= metaData.getColumnCount(); index++)
                        types.put(metaData.getColumnName(index), metaData.getColumnType(index));
                }
            });
        });

        return types;
    }

    private static String rawColumn(@NotNull String column, int id) {
        String[] holder = new String[1];

        database.with(s -> {
            s.doWork(connection -> {
                try (
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT \"" + column + "\" FROM \"gson_fixture\" WHERE \"id\" = " + id)
                ) {
                    assertTrue(resultSet.next(), "no row with id " + id);
                    holder[0] = resultSet.getString(1);
                }
            });
        });

        return holder[0];
    }

    private static void execute(@NotNull String sql) {
        database.with(s -> {
            s.doWork(connection -> {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(sql);
                }
            });
        });
    }

    private static @NotNull GsonFixtureModel load(int id) {
        try (Session scoped = database.openSession()) {
            GsonFixtureModel row = scoped.find(GsonFixtureModel.class, id);
            assertNotNull(row, "no row with id " + id);
            return row;
        }
    }

    private static void persist(int id, @NotNull Consumer<GsonFixtureModel> populate) {
        GsonFixtureModel row = new GsonFixtureModel();
        row.setId(id);
        populate.accept(row);
        database.transaction(s -> { s.persist(row); });
    }

    private static @NotNull GsonFixtureModel roundTrip(int id, @NotNull Consumer<GsonFixtureModel> populate) {
        persist(id, populate);
        return load(id);
    }

    private static Object elementOf(@NotNull List<?> list) {
        assertFalse(list.isEmpty(), "expected a non-empty list");
        return list.getFirst();
    }

    private static Object valueOf(@NotNull Map<?, ?> map, @NotNull Object key) {
        Object value = map.get(key);
        assertNotNull(value, "no entry under '" + key + "'");
        return value;
    }

    private static <K, V> @NotNull ConcurrentMap<K, V> map(@NotNull K key, @NotNull V value) {
        ConcurrentMap<K, V> map = Concurrent.newMap();
        map.put(key, value);
        return map;
    }

    private static @NotNull ConcurrentMap<Integer, Double> values(int level, double amount) {
        return map(level, amount);
    }

}
