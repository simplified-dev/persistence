# Known open

Open items on `feat/indexing` after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/).

> #### A registered relational type reads stale after a raw Hibernate write
> A repository holds one generation of rows and every finder answers from it, so a write that reaches
> the database without going through `JpaSession.write(WriteRequest)` leaves the held rows describing
> the state before it. With `@Hydration` absent - which is the default, meaning hydrate once - they
> stay that way for the life of the session.
>
> `JpaSession.write` is the supported path and closes this: it applies the write through the type's
> `Source.Writable` and then rebuilds that type. `SessionManager.write` finds the session holding the
> type and does the same. `JpaSession.with(...)`, `JpaSession.transaction(...)` and
> `JpaSession.openSession()` do not, and are the escape hatch precisely because they bypass the
> library - so a caller using them against a **registered** type owns the staleness.
>
> Two ways to avoid it, both available today: write through `JpaSession.write`, or do not register the
> type at all and reach it only through the session, which is what registration being the choice means.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java` - `write(WriteRequest)`,
>   `with(Consumer)`, `with(Function)`, `transaction(Consumer)`, `transaction(Function)`,
>   `openSession()`; `src/main/java/dev/simplified/persistence/JpaRepository.java` - `getRows()`
> - Type: **RISK**
> - Status: **OPEN** - inherent to holding a generation, accepted deliberately

> #### Nothing resolves standalone until collections is published
> `build.gradle.kts:21` pins `com.github.simplified-dev:collections` at `strictly("9696ca5")`, and
> that coordinate does not resolve to what this branch needs.
>
> At `collections` `9696ca5` the query package holds four files and there is no `Indexable`, no
> `IndexCache` and no `@Indexed`, so `Repository.indexes()` does not compile rather than merely not
> helping. The sha that carries the indexing surface is `5df6ece` on `collections`' `feat/indexing`,
> which is 21 commits ahead of `origin/master` and unpushed. The same pin appears in
> `Simplified-Api/github/build.gradle.kts`, `Simplified-Api/skyblock/build.gradle.kts`,
> `Simplified-Api/hypixel/build.gradle.kts`, `SkyBlock-Simplified/api/build.gradle.kts` and
> `SkyBlock-Simplified/data/build.gradle.kts`.
>
> Everything therefore verifies only through the root composite at `W:/Workspace/Java/Simplified`,
> which substitutes the local projects:
>
> ```
> ./gradlew :Simplified-Dev:persistence:test :Simplified-Api:github:test \
>   :Simplified-Api:skyblock:test :Simplified-Api:hypixel:test \
>   :SkyBlock-Simplified:api:test :SkyBlock-Simplified:data:test
> ```
>
> Closing it is a sequence on a third-party service, not a line in a build file: push `collections`,
> get a JitPack build, then move the pins and confirm each module resolves the published surface
> rather than the composite's substitution masking it.
>
> - Affected: `build.gradle.kts:21`; `Simplified-Api/github/build.gradle.kts`;
>   `Simplified-Api/skyblock/build.gradle.kts`; `Simplified-Api/hypixel/build.gradle.kts`;
>   `SkyBlock-Simplified/api/build.gradle.kts`; `SkyBlock-Simplified/data/build.gradle.kts`;
>   `Simplified-Dev/collections` branch `feat/indexing` at `5df6ece`
> - Type: **GAP**
> - Status: **OPEN** - needs one push and one third-party build

> #### A single-valued link that resolves to nothing is set to null in silence
> `@Linked` on a non-collection field resolves the id its argument names against the target type's
> held rows. When the id names no row, the field is set to `null` - including where the field is
> declared `@NotNull`, as `Item.category` is, and `Item.equals` reads it. Hibernate's
> `optional = false` used to answer this by refusing the load; nothing answers it now.
>
> The corpus does not currently exercise it: every single-valued link resolves across all 34
> documents, which is what makes it a latent hazard rather than a live defect. `02-flow.md` §5.4 says
> the type should fail; the spine reserved the decision and it is still reserved.
>
> - Affected: `Simplified-Dev/persistence/src/main/java/dev/simplified/persistence/JpaRepository.java`
>   - `resolveLinks`
> - Type: **RISK**
> - Status: **OPEN** - the policy is undecided, per spine §12

> #### `bot` does not build, for two reasons that predate this work
> `SkyBlock-Simplified/bot` cannot be compiled in this workspace, so the three write sites migrated on
> this branch are unverified beyond review.
>
> `Minecraft-Library/asset-renderer` is on `feat/entity-pose` with an uncommitted tree and a missing
> `lib.minecraft.renderer.client` package, producing 19 compile errors in a module `bot` depends on
> transitively. Separately, `bot`'s own `Solution.java` imports `BonusReforgeStat`, for which no file
> exists. Neither is reachable from this design.
>
> `bot` also carries 70 uncommitted files from an unrelated in-progress pass, so `daa4582` commits the
> two this branch had to touch and nothing else.
>
> - Affected: `Minecraft-Library/asset-renderer` branch `feat/entity-pose`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/optimizer/modules/common/Solution.java:11`
> - Type: **GAP**
> - Status: **OPEN** - neither cause belongs to this design

> #### The bot's relational session registers no types
> `JpaConfig.commonSql()` supplies no repository factory, so `build()` falls through to
> `RepositoryFactory.of(JpaModel.class)`, whose scan is anchored in `dev.simplified.persistence` - a
> package that now holds no models at all. A session built from it holds zero repositories, so
> `SkyBlockData.write` against `AppUser` or `AppGuildReputation` would find no session holding the type.
>
> `commonSql()` itself is repaired on this branch: it reads `DATABASE_HOST`, `DATABASE_PORT`,
> `DATABASE_SCHEMA`, `DATABASE_USER` and `DATABASE_PASSWORD`, where before it passed the host variable
> as the schema and set no host, and threw before it could build under any environment. What is left
> is the registration, which under D12 is the whole choice and belongs to the caller: `SimplifiedBot`
> has to name the anchor its models sit under.
>
> - Affected: `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/SimplifiedBot.java:49`
> - Type: **GAP**
> - Status: **OPEN** - out of this pass's scope, and `bot` does not build to verify it

> #### Nothing has measured the second-level cache against the in-memory index
> D12 says registration is the choice: a type in `getModels()` holds a generation, and a relational
> type outside it is reached through the session's Hibernate access. Which relational types belong in
> the list is a measurement, and the harness does not exist - no JMH block, no JOL, no heap-dump step.
> `06-risks-and-measurement.md` §1 specifies what to build.
>
> The corpus memory budget is the same shape of question: 7,593 rows over 237 mapped columns from
> 8.69 MB of JSON, held live in every consuming JVM. `06` §2 estimates roughly 40 MB with its method
> shown, and D2 rests on that being affordable.
>
> - Affected: the design as a whole
> - Type: **GAP**
> - Status: **OPEN** - deliberately, and out of scope for this pass
