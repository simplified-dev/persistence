# Known open

Open items on `feat/indexing` during the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/).

> #### A registered relational type reads stale after a raw Hibernate write
> A repository holds one generation of rows and every finder answers from it, so a write that reaches
> the database without going through `JpaSession.write(WriteRequest)` leaves the held rows describing
> the state before it. With `@Hydration` absent - which is the default, meaning hydrate once - they
> stay that way for the life of the session.
>
> `JpaSession.write` is the supported path and closes this: it applies the write through the type's
> `Source.Writable` and then rebuilds that type. `JpaSession.with(...)` and
> `JpaSession.transaction(...)` do not, and are the escape hatch precisely because they bypass the
> library - so a caller using them against a **registered** type owns the staleness.
>
> Two ways to avoid it, both available today: write through `JpaSession.write`, or do not register the
> type at all and reach it only through the session, which is what registration being the choice means.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java` - `write(WriteRequest)`,
>   `with(Consumer)`, `with(Function)`, `transaction(Consumer)`, `transaction(Function)`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java` - `getRows()`
> - Type: **RISK**
> - Status: **OPEN** - inherent to holding a generation, accepted deliberately

> #### Nothing compiles standalone until the collections branch is published
> `build.gradle.kts:21` pins `com.github.simplified-dev:collections` at `strictly("9696ca5")`. At that
> sha the `query` package holds four files and there is no `Indexable`, no `IndexCache` and no
> `@Indexed`, so `Repository.indexes()` does not compile rather than merely not helping.
>
> The sha that carries the indexing surface is `5df6ece` on `collections`' `feat/indexing`, which is 21
> commits ahead of `origin/master` and **unpushed** - `git branch -a` lists only `origin/master`, so it
> is unfetchable rather than merely unpinned, and JitPack has never built it.
>
> Everything therefore verifies only through the root composite at `W:/Workspace/Java/Simplified`,
> which substitutes the local projects:
>
> ```
> ./gradlew :Simplified-Dev:persistence:test :Simplified-Api:skyblock:compileTestJava
> ```
>
> Closing it is three actions, not one line: push the branch, get a JitPack build, then move the pin
> and confirm it resolves the indexing surface rather than the composite substitution masking it.
>
> - Affected: `build.gradle.kts:21`; `Simplified-Dev/collections` branch `feat/indexing` at `5df6ece`
> - Type: **GAP**
> - Status: **OPEN** - needs a push and a third-party build
