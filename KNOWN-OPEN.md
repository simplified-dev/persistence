# Known open

Open items on `feat/indexing` after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/), and the
ownership of the connect and hydrate path in [`notes/connection-flow/`](notes/connection-flow/).

> #### A registered type reads stale after a write that bypasses its session
> A repository holds one generation of rows and every finder answers from it, so a write that reaches
> the origin without going through `JpaSession.write(WriteRequest)` leaves the held rows describing
> the state before it. With `@Hydration` absent - which is the default, meaning no background cadence -
> they stay that way until something rebuilds the type.
>
> `JpaSession.write` is the supported path and closes this: it applies the write through the session's
> `Source.Writable` and then rebuilds the type and every type linking into it. `SessionManager.write`
> finds the session holding the type and does the same. The caller that opened a database holds the
> `RelationalSource` itself, and its `with`, `transaction`, `openSession` and `write` do not rebuild -
> they are the escape hatch precisely because they bypass the library. The same holds for a
> `DocumentSource.Writable` a caller built and kept, and for `JpaConfig.source()`. A caller writing a
> **registered** type through any of them owns the staleness.
>
> Two ways to avoid it: write through the session, or leave the type out of `JpaConfig.models()` and
> reach it only through the database's Hibernate access, which is what registration being the choice
> means.
>
> - Affected: `src/main/java/dev/simplified/persistence/source/RelationalSource.java` - `write` at
>   `:206`, `openSession` at `:233`, `with` at `:243` and `:260`, `transaction` at `:275` and `:291`;
>   `src/main/java/dev/simplified/persistence/source/DocumentSource.java:133`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java:119`
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
> rows. When the id names no row, the field is set to `null` - including where the field is declared
> `@NotNull`, as `Item.category` is, and `Item.equals` reads it. Hibernate's `optional = false` used to
> answer this by refusing the load; nothing answers it now.
>
> The corpus does not currently exercise it: every single-valued link resolves across all 34
> documents, which is what makes it a latent hazard rather than a live defect. `02-flow.md` §5.4 says
> the type should fail; the spine reserved the decision and it is still reserved.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaRepository.java:213` - `resolveLinks`
> - Type: **RISK**
> - Status: **OPEN** - the policy is undecided, per spine §12

> #### `@Hydration(blocking = false)` changes nothing
> `01-contracts.md` §6 gives `blocking()` one job - whether `SessionManager.connect(...)` waits for a
> type's first generation before returning - and gives a reader a fixed contract: block on
> `UNHYDRATED` and `HYDRATING`, throw on `FAILED`, return on everything else.
>
> `connect` now hydrates every registered type before it registers the session, so no lookup through
> a `SessionManager` ever reaches a repository in `UNHYDRATED` or `HYDRATING`, and a rebuild publishes
> nothing until its links resolve. That makes every type behave as `blocking = true`. The element is
> still declared and still read by nothing, so `blocking = false` - a type whose readers would rather
> wait on first access than hold up the connect - is unbuilt, as is the prior pack's O2, first
> hydration off the calling thread.
>
> - Affected: `src/main/java/dev/simplified/persistence/Hydration.java:56`;
>   `src/main/java/dev/simplified/persistence/JpaSession.java:102` - `cacheRepositories()`
> - Type: **GAP**
> - Status: **OPEN** - the non-blocking startup is unbuilt

> #### A write that lands can still throw, and the queue re-applies it
> A write rebuilds the written type and every type linking into it, after the origin has accepted the
> write. A write to `Item` re-reads six documents, one to `Region` eight. If any of them fails to read,
> `JpaSession.write` throws even though the write itself landed, and the whole rebuild publishes
> nothing, so the written type keeps its pre-write generation too. `WriteQueueConsumer` treats the
> throw as a failed write, reschedules it and re-applies it - one more commit per retry - until the
> retry cap dead-letters it.
>
> Nothing retries the rebuild itself. A type without a `@Hydration` cadence that fails to rebuild
> stays `DEGRADED`, serving its pre-write rows, until another write reaches its rebuild set; the corpus
> writer recovers only because the queue re-applies the write.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java` - `write(WriteRequest)`,
>   `hydrate(ConcurrentList)`; `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:208-211`
> - Type: **RISK**
> - Status: **OPEN** - the write and its rebuild report through one exception, and a failed rebuild is
>   not retried

> #### The caller owns closing a database, including after a failed connect
> A session never opens or closes its source. `SessionManager.connect` shuts down a session whose first
> hydration throws, but the database the caller opened for it stays open - its pool, its service
> registry and its cache regions - until the caller closes it. The caller also owns the order: closing
> the database before shutting the session down lets a due tick or a write reach a closed session
> factory, and a database shared by two sessions has to outlive both.
>
> - Affected: `src/main/java/dev/simplified/persistence/SessionManager.java:46` - `connect(JpaConfig)`;
>   `src/main/java/dev/simplified/persistence/source/RelationalSource.java:305` - `close()`
> - Type: **RISK**
> - Status: **OPEN** - documented on `connect` and `open`; nothing enforces it

> #### The background cadence has never run
> No model or fixture in the workspace declares `@Hydration`, so the scheduler a session builds for a
> cadence, `hydrateDue`, `isDue`, `isPastStaleness`, `markStale` and the `STALE` state have never
> executed, and no test would notice if they broke. A tick that fails is also silent: the exception
> leaves `hydrateDue` into the scheduler, which only counts it. Whether the cadence stays at all is
> undecided - every current consumer is rebuilt by writes alone.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java` - `cacheRepositories()`
>   scheduling at `:121`, `hydrateDue()` at `:198`; `src/main/java/dev/simplified/persistence/Hydration.java`
> - Type: **GAP**
> - Status: **OPEN** - keep and test it, or delete it

> #### A collection-valued association is not followed by the rebuild rule
> A write rebuilds every type linking into the written one through a `@Linked` field, a `@ManyToOne`
> or a `@OneToOne`. A `@OneToMany` or `@ManyToMany` is not followed, because its target is read off
> the declared element type, which a raw, wildcard or map-typed collection does not give. No model in
> the workspace declares one; the first that does keeps the pre-write rows of its collection after a
> write to the element type.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java:306` - `dependentsOf`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java` - `targetOf`
> - Type: **GAP**
> - Status: **OPEN** - no model needs it yet

> #### Closing one database destroys the cache regions another open database uses
> Every `JpaCacheProvider` names no configuration resource, so every database a process opens shares
> the provider's one cache manager. Regions are named after the mapped type, or are one of the two
> shared query-cache names, and opening reuses a region that already exists - with the TTL it was
> first created with. Closing destroys every region its mapped types and the query cache name, so with
> two databases open over overlapping types, closing either empties the other's regions. No caller
> opens two databases today.
>
> - Affected: `src/main/java/dev/simplified/persistence/source/RelationalSource.java` -
>   `destroyRegions()` at `:339`, `buildCacheConfiguration(String, Duration)` at `:528`,
>   `resolveCacheManager()` at `:548`
> - Type: **RISK**
> - Status: **OPEN** - per-database region prefixes would separate them

> #### The connection password is readable from an open database's session factory
> `RelationalSource` keeps its credentials in a private record and builds its settings map as a
> constructor local, so neither is reachable through it. Hibernate keeps the settings it was built
> from, though, and `getSessionFactory().getProperties()` answers them - `hibernate.connection.password`
> included - to anyone holding the source or a `JpaConfig` over it.
>
> - Affected: `src/main/java/dev/simplified/persistence/source/RelationalSource.java` - the generated
>   `getSessionFactory()`
> - Type: **RISK**
> - Status: **OPEN** - narrowed, not closed

> #### A shut-down session with a cadence stays reachable until the process exits
> `Scheduler` registers a JVM shutdown hook it never removes, and its `shutdown()` cancels its tasks
> without dropping them. A session's tick is a task holding the session, so a session that declared a
> cadence and was shut down stays reachable through the hook until exit. The fix belongs to the
> scheduler module: clear the task list once the tasks are cancelled.
>
> - Affected: `Simplified-Dev/scheduler/src/main/java/dev/simplified/scheduler/Scheduler.java:83`,
>   `:331-336`
> - Type: **RISK**
> - Status: **OPEN** - latent, since no type declares a cadence

> #### A document write to an overridden key is reverted by its own rebuild
> `DocumentSource.Writable.write` merges every layer, applies the request and rewrites the first layer
> with the whole merged set; the later layers are untouched. The rebuild that follows merges again and
> the later layer wins, so an upsert of a key an override layer carries is reverted, and a delete of
> one reappears. The copy into the first layer is pinned as intended by
> `DocumentLayerMergeTest.writeCarriesTheWholeDocument`; the revert is not tested.
>
> - Affected: `src/main/java/dev/simplified/persistence/source/DocumentSource.java:133` - `write`
> - Type: **BUG**
> - Status: **OPEN**

> #### A document write can overwrite a concurrent commit
> No production write names a precondition, so `CorpusOrigin.Writing` asks GitHub for the file's
> current blob sha at the moment it writes - after the merge read. A commit landing between the two
> reads is overwritten with a merge of the older content, where `DocumentOrigin.Writable`'s javadoc
> says a moved path refuses the write.
>
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/CorpusOrigin.java:98`;
>   `src/main/java/dev/simplified/persistence/source/DocumentOrigin.java:56-59`
> - Type: **BUG**
> - Status: **OPEN**

> #### A consumer can still force a full SkyBlock rehydration
> An empty write no longer rebuilds anything and `SessionManager.reconnect` is gone, but
> `SkyBlockData.getSessionManager()` is public, so `shutdown()` followed by `SkyBlockData.connect()`
> re-reads every type - which D7 and invariant 5 say no downstream consumer can do.
>
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:44`,
>   `:87`
> - Type: **RISK**
> - Status: **OPEN**

> #### A moved document has no route into a session
> The writer's poller computes which corpus documents moved and discards the answer, and the poll
> swaps the catalogue the writer's `CorpusOrigin` reads. A session learns of a moved document only by
> writing it or by a `@Hydration` tick, and no corpus type declares one. The rebuild rule says what to
> rebuild once a moved type is known - that type and every type linking into it - but not how the
> session is told: a fourth `DocumentOrigin` question the session asks, a fingerprint entry the
> deployment calls, which invariant 5 requires to rebuild nothing when repeated, or not at all.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/poller/CorpusPoller.java` -
>   `scheduled()` at `:66-76` discards what `poll()` at `:83-106` returns;
>   `src/main/java/dev/simplified/persistence/source/DocumentOrigin.java`
> - Type: **GAP**
> - Status: **OPEN** - the route is undecided

> #### `bot` does not build, for two reasons that predate this work
> `SkyBlock-Simplified/bot` cannot be compiled in this workspace, so its write sites and its
> relational session are unverified beyond review.
>
> `Minecraft-Library/asset-renderer` is on `feat/entity-pose` with an uncommitted tree and a missing
> `lib.minecraft.renderer.client` package, producing 19 compile errors in a module `bot` depends on
> transitively. Separately, `bot`'s own `Solution.java` imports `BonusReforgeStat`, for which no file
> exists. Neither is reachable from this design.
>
> `bot` also carries an uncommitted tree from an unrelated in-progress pass. `SimplifiedBot`,
> `TestLifecycleListener` and the untracked `JpaExtractorStore` are part of it, so the lines this
> design changed in them - `SkyBlockData.connect()` without settings, the bot opening and registering
> its own database, `JpaExtractorStore` holding a `RelationalSource` - sit uncommitted in that tree.
>
> Once it builds, three things stand between it and a working database. Two of its models carry
> mappings Hibernate refuses when the database opens: `OptimizerSupportItem` keys on a `@ManyToOne` to
> the corpus's `Item`, which no bot database maps, and `SkyBlockEventTimer` puts `@ManyToOne` on the
> enum `Season`. `JpaExtractor` sits outside the package the bot's models are discovered from, so the
> database does not map it and nothing installs `JpaExtractorStore`. And `TestLifecycleListener`
> connects the corpus over GitHub and registers no bot session, where a test run wants the disk
> checkout and an in-memory database over the same models `SimplifiedBot` registers.
>
> - Affected: `Minecraft-Library/asset-renderer` branch `feat/entity-pose`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/optimizer/modules/common/Solution.java:11`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/SimplifiedBot.java`;
>   `SkyBlock-Simplified/bot/src/test/java/dev/sbs/bot/TestLifecycleListener.java`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/feature/extractor/JpaExtractorStore.java`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/persistence/model/OptimizerSupportItem.java:32`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/persistence/model/SkyBlockEventTimer.java:40`, `:49`
> - Type: **GAP**
> - Status: **OPEN** - neither cause belongs to this design

> #### Nothing has measured the second-level cache against the in-memory index
> D12 says registration is the choice: a type in `JpaConfig.models()` holds a generation, and a
> relational type left out of it is reached through the Hibernate access of the database that maps
> it. Which relational types belong in the list is a measurement, and the harness does not exist - no
> JMH block, no JOL, no heap-dump step. `06-risks-and-measurement.md` §1 specifies what to build.
>
> The corpus memory budget is the same shape of question: 7,593 rows over 237 mapped columns from
> 8.69 MB of JSON, held live in every consuming JVM. `06` §2 estimates roughly 40 MB with its method
> shown, and D2 rests on that being affordable.
>
> - Affected: the design as a whole
> - Type: **GAP**
> - Status: **OPEN** - deliberately, and out of scope for this pass
