# Known open

Open items on `feat/indexing` after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/), and the
ownership of the connect and hydrate path in [`notes/connection-flow/`](notes/connection-flow/).

> #### Nothing resolves standalone until collections is published
> `build.gradle.kts:21` pins `com.github.simplified-dev:collections` at `strictly("9696ca5")`, and
> that coordinate does not resolve to what this branch needs.
>
> At `collections` `9696ca5` the query package holds four files and there is no `Indexable`, no
> `IndexCache` and no `@Indexed`, so `Repository.indexes()` does not compile rather than merely not
> helping. The surface arrives at `5df6ece` on `collections`' `feat/indexing`, whose tip `58aaa00` is
> 24 commits ahead of `origin/master` and unpushed. Of the three commits after `5df6ece`, `de13723`
> makes `collections` depend on `utils` `5d14f56`, where persistence pins `utils` `3d8af56`, which still
> depends on `collections` `9696ca5`; and `58aaa00` adds the `Graph.ancestors` that the TODO in
> `JpaSession.dependentsOf` waits on.
>
> The same `collections` pin appears in `Simplified-Api/github/build.gradle.kts`,
> `Simplified-Api/skyblock/build.gradle.kts`, `Simplified-Api/hypixel/build.gradle.kts`,
> `SkyBlock-Simplified/api/build.gradle.kts` and `SkyBlock-Simplified/data/build.gradle.kts`, and in
> thirteen other modules by convention. The four that consume persistence have to move with it: their
> own `strictly` overrides the `requires` persistence publishes, and would hand persistence a
> `collections` without `IndexCache`. They also pin persistence at `2d6b0e7`, its `origin/master`,
> where `WriteRequest` is not generic, so `SkyBlock-Simplified/api` does not compile against its own
> pin. skyblock and data pin github at `b64127c`, which has no `GitHubCorpus`; hypixel and data pin
> skyblock at `d566734`; and data takes `SkyBlock-Simplified/api` as `master-SNAPSHOT`, which has no
> `WriteEnvelope`. persistence, github, skyblock, hypixel, `SkyBlock-Simplified/api` and data are all
> unpushed on `feat/indexing`.
>
> persistence also pins `scheduler` at `21570df`, its `origin/master`, where `Scheduler.shutdown()`
> leaves its JVM shutdown hook registered and so keeps a shut-down session with a `@Hydration` cadence
> reachable until exit. The hook is removed at `4c401ab`, on `scheduler`'s unpushed
> `fix/shutdown-hook`, so a shut-down session is released only where that commit is substituted.
>
> Everything therefore verifies only through the root composite at `W:/Workspace/Java/Simplified`,
> which substitutes the local projects - `utils` at its working tree, `5d14f56`, and `scheduler` at
> `4c401ab` among them, so the composite masks persistence's `utils` and `scheduler` pins as well:
>
> ```
> ./gradlew :Simplified-Dev:scheduler:test :Simplified-Dev:persistence:test \
>   :Simplified-Api:github:test :Simplified-Api:skyblock:test :Simplified-Api:hypixel:test \
>   :SkyBlock-Simplified:api:test :SkyBlock-Simplified:data:test
> ```
>
> Closing it is a sequence on a third-party service, not a line in a build file: build `collections`,
> merge and build `scheduler`, and move persistence's pins; push and build persistence; move
> `SkyBlock-Simplified/api`'s pins, merge it to `master` and build it; push and build github; move
> skyblock's pins, push and build it; then move the hypixel and data pins - confirming at each step
> that the module resolves the published surface rather than the composite's substitution masking it.
>
> - Affected: `build.gradle.kts:21`, `:22`, `:25`; `Simplified-Api/github/build.gradle.kts:37`;
>   `Simplified-Api/skyblock/build.gradle.kts:35`, `:38`, `:39`, `:42`;
>   `Simplified-Api/hypixel/build.gradle.kts:35`, `:38`, `:39`, `:42`;
>   `SkyBlock-Simplified/api/build.gradle.kts:38`, `:39`, `:45`;
>   `SkyBlock-Simplified/data/build.gradle.kts:67`, `:71`, `:76`, `:78`, `:79`;
>   `Simplified-Dev/collections` branch `feat/indexing` at `58aaa00`;
>   `Simplified-Dev/scheduler` branch `fix/shutdown-hook` at `4c401ab`
> - Type: **GAP**
> - Status: **OPEN** - needs pushes and JitPack builds in dependency order, `collections` first

> #### A single-valued link that resolves to nothing is set to null in silence
> `@Linked` on a non-collection field resolves the id its argument names against the target type's
> rows. When the id names no row, the field is set to `null` - including where the field is declared
> `@NotNull`, as `Item.category` is, so the defect surfaces at whichever reader first follows it.
> Hibernate's `optional = false` used to answer this by refusing the load; nothing answers it now.
>
> The corpus does not currently exercise it: every single-valued link that carries an id resolves
> across all 34 documents, which is what makes it a latent hazard rather than a live defect.
> `FairySoul.zone` is not exercised at all, because `fairy_souls.json` holds no rows. The two stone
> links carry no id, because `Reforge.stoneId` and `Power.stoneId` declare no `@SerializedName` while
> the documents spell the key `stone`; bound, two reforge stones name no item. `02-flow.md` §5.4 says
> the type should fail; the spine reserved the decision and it is still reserved.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaRepository.java:241` - `resolveLinks`, which
>   assigns the miss at `:267`
> - Type: **RISK**
> - Status: **OPEN** - the policy is undecided, per spine §12

> #### A write that lands can still throw, and the queue re-applies it
> A write rebuilds the written type and every type linking into it, after the origin has accepted the
> write. A write to `Item` re-reads six documents, one to `Region` eight. If any of them fails to read
> or link, `JpaSession.write` throws even though the write itself landed, and the whole rebuild
> publishes nothing, so the written type keeps its pre-write generation too. `WriteQueueConsumer`
> treats the throw as a failed write, reschedules it and re-applies it - one more commit per retry -
> until a rebuild succeeds or the retry cap dead-letters a write that landed. Like every write it
> reschedules, a re-applied one re-sends the rows it was enqueued with, so it reverts a later write to
> the same row that lands during the backoff. The bot's `LinkCommand` and `RepGiveCommand` let the
> same throw escape the command for a row that was saved.
>
> Only a cadence retries the rebuild itself, and no corpus type declares one. A type without a
> `@Hydration` cadence that fails to rebuild stays `DEGRADED`, serving its pre-write rows, until
> another write reaches its rebuild set or a type it links into comes due on its own cadence. The
> corpus writer's session serves no reads - `data` connects it on a private `SessionManager` and only
> writes through it - so there the re-apply recovers a generation nobody reads; the stale rows matter
> where a written session is also read, which today is the bot's.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java:265` - `write(WriteRequest)`,
>   `:147` - `hydrate(ConcurrentList)`;
>   `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:192` - `apply`,
>   which reschedules at `:214`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/command/LinkCommand.java:46` - `process`, which
>   writes at `:72` and `:87`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/command/reputation/RepGiveCommand.java:45` -
>   `process`, which writes at `:84`
> - Type: **RISK**
> - Status: **OPEN** - the write and its rebuild report through one exception, and a failed rebuild is
>   not retried

> #### A collection-valued association is not followed by the rebuild rule
> A write rebuilds every type linking into the written one through a `@Linked` field, or a field
> carrying `@ManyToOne` or `@OneToOne`. Following JPA associations at all departs from the rule the
> connection-flow decisions settled, which saw `@Linked` fields only, and that departure has not been
> reviewed. A `@OneToMany` or `@ManyToMany` is not followed: the edge filter admits neither, and
> `targetOf`, which reads every edge's target, casts the declared element type rather than resolving
> it, so a raw or wildcard collection would throw and a map would yield no edge. No model in the
> workspace declares one. The first that does, read from a database with its default lazy fetch, holds
> a collection whose Hibernate session closed with the read, so it throws on first access whatever is
> written; declared `fetch = EAGER`, it keeps the pre-write rows after a write to the element type.
>
> `targetOf` already meets that limit on the `@Linked` side: a raw or wildcard element type throws a
> `ClassCastException` out of `connect` rather than a `JpaException`. No `@Linked` field in the
> workspace has one.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java:333` - `dependentsOf`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java:319` - `targetOf`;
>   `src/main/java/dev/simplified/persistence/source/RelationalSource.java:239` - `read`
> - Type: **GAP**
> - Status: **OPEN** - no model needs it yet

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
> No production write names a precondition - nothing answers a caller a revision it could name - so
> `CorpusOrigin.Writing` asks GitHub for the file's current blob sha at the moment it writes, after
> the merge read. A commit landing after the body was read is overwritten with a merge of the older
> content. The window is wider than the gap between the two reads: `GitHubCorpus` reads the body
> through a client whose response cache replays a file for up to a minute, and resolves the sha
> through a second client with a cache of its own, so the body can be up to a minute older than the
> sha it is written under. The javadoc of `DocumentOrigin.Writable.write`, and of its override in
> `CorpusOrigin.Writing`, says a moved path refuses the write. `WriteRequest` says a GitHub source
> retries when the origin has moved, and nothing retries. `Source.Writable.write` promises a
> precondition no production write carries.
>
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/CorpusOrigin.java:98` -
>   `Writing.write`, javadoc at `:93-95`;
>   `src/main/java/dev/simplified/persistence/source/DocumentOrigin.java:71` - `Writable.write`, javadoc
>   at `:57-60`;
>   `src/main/java/dev/simplified/persistence/source/WriteRequest.java:30` - `precondition`, class
>   javadoc at `:14-16`;
>   `src/main/java/dev/simplified/persistence/source/Source.java:60` - `Writable.write`, `@throws` at
>   `:57-58`;
>   `Simplified-Api/github/src/main/java/api/simplified/github/GitHubCorpus.java:98` - `read`, `:109` -
>   `metadata`
> - Type: **BUG**
> - Status: **OPEN**

> #### A consumer can still force a full SkyBlock rehydration
> An empty write no longer rebuilds anything and `SessionManager.reconnect` is gone, but
> `SkyBlockData.getSessionManager()` is public, so `shutdown()` followed by `SkyBlockData.connect()`
> re-reads every type - which D7 and invariant 5 say no downstream consumer can do.
>
> The getter is also how the bot registers its own database session beside the corpus session, and
> how the skyblock and hypixel tests connect and disconnect a local checkout, so closing it moves all
> three. Closing it alone does not stop a second `SkyBlockData.connect()`: `SessionManager.connect`
> does not refuse a type an active session already registers, so the second call re-reads every type
> into a session behind the first, which every lookup and write still reaches first.
>
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:45` -
>   `sessionManager` and its generated getter, `:95` - `connect()`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/SimplifiedBot.java:48` - `main`;
>   `SkyBlock-Simplified/bot/src/test/java/dev/sbs/bot/TestLifecycleListener.java:19` -
>   `testPlanExecutionStarted`, `:26` - `testPlanExecutionFinished`;
>   `Simplified-Api/skyblock/src/test/java/api/simplified/skyblock/LocalSkyBlockData.java:92` -
>   `connect`, `:104` - `disconnect`;
>   `Simplified-Api/hypixel/src/test/java/api/simplified/hypixel/response/skyblock/stats/LocalSkyBlockData.java:110`
>   - `connect`, `:122` - `disconnect`
> - Type: **RISK**
> - Status: **OPEN**

> #### A moved document has no route into a session
> The writer's poller computes which corpus documents moved and discards the answer, and the poll
> swaps the catalogue the writer's `CorpusOrigin` reads. That session serves no reads, though - `data`
> answers only `/actuator/prometheus` - and the sessions that do, the read-only ones
> `SkyBlockData.connect()` registers in `bot`, have no poller at all. A session learns of a moved
> document only when a write's rebuild covers its type or a `@Hydration` tick comes due, and no corpus
> type declares one. The rebuild rule says what to rebuild once a moved type is known - that type
> and every type linking into it - but not how the session is told: a fourth `DocumentOrigin` question
> the session asks, a fingerprint entry the deployment calls, which invariant 5 requires to rebuild
> nothing when repeated, or not at all.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/poller/CorpusPoller.java` -
>   `scheduled()` at `:66-77` discards what `poll()` at `:84-111` returns;
>   `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:95` - `connect()`;
>   `src/main/java/dev/simplified/persistence/source/DocumentOrigin.java`
> - Type: **GAP**
> - Status: **OPEN** - the route is undecided

> #### `bot` does not build, for reasons that predate this work
> `SkyBlock-Simplified/bot` cannot be compiled in this workspace, so its write sites and its
> relational session are unverified beyond review.
>
> Compiled through the root composite, `bot` fails with 67 errors across 18 of its own files, none of
> them naming anything this work removed. Six - `TreePage` and `ItemHandler` - name types
> `discord4j-framework` removes only on its unpushed `offline-test-harness` branch, which the
> composite substitutes for the `master-SNAPSHOT` `bot` declares. The rest name removals already on a
> published `master` - `collections`' `unmodifiable` package, `hypixel`'s `profile_stats`, skyblock's
> retired bonus-stat models - or `nbt-factory` subpackages it never had. `LinkCommand` and
> `RepGiveCommand` are committed, so `HEAD` fails too. Every error is an import or a signature, so
> javac never reached a method body and more may follow. `Minecraft-Library/asset-renderer`, which
> `bot` declares directly, compiles, and nothing in `bot` imports it.
>
> `bot` also carries an uncommitted tree from an unrelated in-progress pass. `SimplifiedBot`,
> `TestLifecycleListener` and the untracked `JpaExtractorStore` are part of it, so the lines this
> design changed in them - `SkyBlockData.connect()` without settings, the bot opening and registering
> its own database, `JpaExtractorStore` holding a `RelationalSource` - sit uncommitted in that tree.
> The same pass restores `Solution` and `OptimizerTest`, which are disabled stubs at `HEAD`, and 16 of
> the 18 failing files are in it; the restored `OptimizerTest` also calls `MinecraftApi`,
> `JpaConfig.commonSql()`, `getInitialization()` and `getRepositoryCache()`, none of which exists.
>
> Once it builds, three things stand between it and a working database. Two of its models carry
> mappings Hibernate refuses when the database opens: `OptimizerSupportItem` keys on a `@ManyToOne` to
> the corpus's `Item`, which no bot database maps, and `SkyBlockEventTimer` puts `@ManyToOne` on the
> enum `Season`. `JpaExtractor` sits outside the package the bot's models are discovered from, so the
> database does not map it and nothing installs `JpaExtractorStore`. And `TestLifecycleListener`
> connects the corpus over GitHub and registers no bot session, where a test run wants the disk
> checkout and an in-memory database over the same models `SimplifiedBot` registers.
>
> - Affected: `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/optimizer/modules/common/Solution.java:9-11`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/command/LinkCommand.java:12`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/command/reputation/RepGiveCommand.java:8`;
>   `SkyBlock-Simplified/bot/build.gradle.kts:60`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/SimplifiedBot.java`;
>   `SkyBlock-Simplified/bot/src/test/java/dev/sbs/bot/TestLifecycleListener.java`;
>   `SkyBlock-Simplified/bot/src/test/java/dev/sbs/bot/optimizer/OptimizerTest.java:33`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/feature/extractor/JpaExtractorStore.java`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/persistence/model/OptimizerSupportItem.java:34`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/persistence/model/SkyBlockEventTimer.java:42`, `:51`
> - Type: **GAP**
> - Status: **OPEN** - no cause belongs to this design

> #### Nothing has measured the second-level cache against the in-memory index
> D12 says registration is the choice: a type in `JpaConfig.models()` holds a generation, and a
> relational type left out of it is reached through the Hibernate access of the database that maps
> it. Which relational types belong in the list is a measurement, and the harness does not exist - no
> JMH block, no JOL, no heap-dump step. `06-risks-and-measurement.md` §1 specifies what to build,
> though its fixture recipe names the API before the unification - `JpaConfig.common(...)`, a
> `SessionFactory` on the session - where a database is now opened through its driver and holds its
> own. The in-memory side indexes nothing yet either: no model in the workspace declares `@Indexed`,
> so every finder scans, although `SkyBlockData.getRepository`'s javadoc promises a hash probe. The one
> relational consumer, `bot`, registers all 23 of its tables in its uncommitted tree, and does not
> build.
>
> The corpus memory budget is the same shape of question: 7,593 rows over 237 mapped columns from
> 8.69 MB of JSON, held live in every consuming JVM. `06` §2 estimates roughly 40 MB with its method
> shown, and D2 rests on that being affordable.
>
> - Affected: the design as a whole; `build.gradle.kts:1-3` and `gradle/libs.versions.toml`, which
>   declare no benchmark source set
> - Type: **GAP**
> - Status: **OPEN** - deliberately, and out of scope for this pass
