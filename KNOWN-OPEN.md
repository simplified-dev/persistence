# Known open

Open items after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/), and the
ownership of the connect and hydrate path in [`notes/connection-flow/`](notes/connection-flow/).

> #### Two writes running at the same time can pass the link check together
> `JpaSession.write` runs its dangling-link check under the session's lock and releases the lock
> before the write reaches the source, so two writes through one session running at the same time
> are each checked against the rows the session held before either landed. An upsert naming a row
> and a delete of that row can both pass and both land, leaving the upserted row's plain `@Linked`
> field naming nothing, which fails every rebuild covering it and every connect after it until the
> data is repaired. Two `JpaConfig.write` calls over one source race the same way, each checking the
> rows the source answered before the other committed. Holding the lock through the write would hold
> it through the source's I/O. `JpaSession.write`'s javadoc states the race.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java:416` - `write`;
>   `src/main/java/dev/simplified/persistence/JpaConfig.java:79` - `write`
> - Type: **RISK**
> - Status: **OPEN**
