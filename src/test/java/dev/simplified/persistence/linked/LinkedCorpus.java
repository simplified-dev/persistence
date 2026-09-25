package dev.simplified.persistence.linked;

import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.optional.LinkedStray;
import dev.simplified.persistence.sibling.LinkedSibling;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import dev.simplified.reflection.Reflection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A writable source held in memory over the linked models, and over the {@link LinkedStray} and
 * {@link LinkedSibling} rows kept outside their package, answering fresh instances on every read the
 * way a parsed document does.
 *
 * <p>It counts reads per type, can be told to fail one type's read, to answer a parent row that
 * carries no id or to refuse a write, and can park a parent read behind a gate while it records how
 * many reads were ever in flight at once.
 */
public final class LinkedCorpus implements Source.Writable {

    /**
     * Parent names, keyed by parent id.
     */
    public final @NotNull ConcurrentMap<String, String> parents = Concurrent.newLinkedMap();

    /**
     * Parent ids, keyed by child id.
     */
    public final @NotNull ConcurrentMap<String, String> children = Concurrent.newLinkedMap();

    /**
     * Child ids, keyed by grandchild id.
     */
    public final @NotNull ConcurrentMap<String, String> grandchildren = Concurrent.newLinkedMap();

    /**
     * Parent ids, empty where the row names none, keyed by stray id.
     */
    public final @NotNull ConcurrentMap<String, Optional<String>> strays = Concurrent.newLinkedMap();

    /**
     * Sibling ids, keyed by sibling id.
     */
    public final @NotNull ConcurrentMap<String, String> siblings = Concurrent.newLinkedMap();

    /**
     * The reads answered so far, per type.
     */
    private final @NotNull ConcurrentMap<Class<?>, AtomicInteger> reads = Concurrent.newMap();

    /**
     * The reads running right now.
     */
    private final @NotNull AtomicInteger inFlight = new AtomicInteger();

    /**
     * The most reads that were ever running at once.
     */
    public final @NotNull AtomicInteger maxInFlight = new AtomicInteger();

    /**
     * The type whose read throws, or {@code null} for none.
     */
    public volatile @Nullable Class<?> failing;

    /**
     * Whether a parent read also answers one row carrying no id.
     */
    public volatile boolean parentWithoutId;

    /**
     * Whether a write is refused before it changes anything.
     */
    public volatile boolean refusing;

    /**
     * The gate a parent read waits behind, or {@code null} for none.
     */
    public volatile @Nullable CountDownLatch gate;

    /**
     * Counted down once a parent read has reached the gate.
     */
    public final @NotNull CountDownLatch parked = new CountDownLatch(1);

    /**
     * Counts the reads answered for one type.
     *
     * @param type the type asked about
     * @return how many reads of it were answered
     */
    public int readsOf(@NotNull Class<?> type) {
        AtomicInteger count = this.reads.get(type);
        return count == null ? 0 : count.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends JpaModel> @NotNull ConcurrentList<T> read(@NotNull Class<T> type) {
        this.maxInFlight.accumulateAndGet(this.inFlight.incrementAndGet(), Math::max);

        try {
            CountDownLatch gate = this.gate;

            if (gate != null && type == LinkedParent.class) {
                this.parked.countDown();

                if (!gate.await(10, TimeUnit.SECONDS))
                    throw new IllegalStateException("The gate was never opened");
            }

            if (type == this.failing)
                throw new IllegalStateException(String.format("Source down for '%s'", type.getSimpleName()));

            this.reads.computeIfAbsent(type, key -> new AtomicInteger()).incrementAndGet();
            ConcurrentList<JpaModel> rows = Concurrent.newList();

            if (type == LinkedParent.class) {
                this.parents.forEach((id, name) -> rows.add(parent(id, name)));

                if (this.parentWithoutId) {
                    LinkedParent anonymous = parent("", "anonymous");
                    new Reflection<>(LinkedParent.class).getField("id").set(anonymous, null);
                    rows.add(anonymous);
                }
            } else if (type == LinkedChild.class)
                this.children.forEach((id, parentId) -> rows.add(child(id, parentId)));
            else if (type == LinkedGrandchild.class)
                this.grandchildren.forEach((id, childId) -> rows.add(grandchild(id, childId)));
            else if (type == LinkedStray.class)
                this.strays.forEach((id, parentId) -> rows.add(stray(id, parentId.orElse(null))));
            else if (type == LinkedSibling.class)
                this.siblings.forEach((id, siblingId) -> rows.add(sibling(id, siblingId)));

            return (ConcurrentList<T>) rows;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(exception);
        } finally {
            this.inFlight.decrementAndGet();
        }
    }

    @Override
    public <T extends JpaModel> void write(@NotNull WriteRequest<T> request) throws JpaException {
        if (this.refusing)
            throw new JpaException("The origin refused '%s'", request.type().getSimpleName());

        request.rows().forEach(row -> {
            if (row instanceof LinkedParent parent)
                this.parents.put(parent.getId(), parent.getName());
            else if (row instanceof LinkedChild child)
                this.children.put(child.getId(), child.getParentId());
            else if (row instanceof LinkedGrandchild grandchild)
                this.grandchildren.put(grandchild.getId(), grandchild.getChildId());
            else if (row instanceof LinkedStray stray)
                this.strays.put(stray.getId(), stray.getParentId());
            else if (row instanceof LinkedSibling sibling)
                this.siblings.put(sibling.getId(), sibling.getSiblingId());
        });
    }

    /**
     * Builds a parent row.
     *
     * @param id the parent's id
     * @param name the parent's name
     * @return the row
     */
    public static @NotNull LinkedParent parent(@NotNull String id, @NotNull String name) {
        LinkedParent parent = new LinkedParent();
        parent.setId(id);
        parent.setName(name);
        return parent;
    }

    /**
     * Builds a child row.
     *
     * @param id the child's id
     * @param parentId the id of the parent it links to
     * @return the row
     */
    public static @NotNull LinkedChild child(@NotNull String id, @NotNull String parentId) {
        LinkedChild child = new LinkedChild();
        child.setId(id);
        child.setParentId(parentId);
        return child;
    }

    /**
     * Builds a grandchild row.
     *
     * @param id the grandchild's id
     * @param childId the id of the child it links to
     * @return the row
     */
    public static @NotNull LinkedGrandchild grandchild(@NotNull String id, @NotNull String childId) {
        LinkedGrandchild grandchild = new LinkedGrandchild();
        grandchild.setId(id);
        grandchild.setChildId(childId);
        return grandchild;
    }

    /**
     * Builds a stray row.
     *
     * @param id the stray's id
     * @param parentId the id of the parent it links to, or {@code null} for none
     * @return the row
     */
    public static @NotNull LinkedStray stray(@NotNull String id, @Nullable String parentId) {
        LinkedStray stray = new LinkedStray();
        stray.setId(id);
        stray.setParentId(Optional.ofNullable(parentId));
        return stray;
    }

    /**
     * Builds a sibling row.
     *
     * @param id the sibling's id
     * @param siblingId the id of the sibling it links to
     * @return the row
     */
    public static @NotNull LinkedSibling sibling(@NotNull String id, @NotNull String siblingId) {
        LinkedSibling sibling = new LinkedSibling();
        sibling.setId(id);
        sibling.setSiblingId(siblingId);
        return sibling;
    }

}
