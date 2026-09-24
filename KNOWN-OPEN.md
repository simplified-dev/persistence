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
> `fix/shutdown-hook`, so a shut-down session is released only where that commit is substituted, and
> `:25` has to move to the sha that branch builds.
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
> Closing it is the full one-sha cascade, a sequence on a third-party service rather than a line in a
> build file: every module `toolsmith jitpack order collections` lists ends up pinning one sha per
> artifact. Resolution needs only the first of its three passes - an inherited pin is published as
> `requires`, which a consumer's own `strictly` overrides - and the other two keep that convention and
> leave no published jar compiled against `9696ca5` running against the new `collections`. Every push
> and JitPack build in it is the user's. Before a sha is pinned,
> `toolsmith jitpack status <module> --ref <sha>` confirms it built, and each module is verified
> outside the composite with `toolsmith gradle verify <module> test`, so it resolves the published
> surface rather than the substitution.
>
> 1. The chain. Finish `collections`' `feat/indexing` into `master` (`toolsmith branch finish`, a merge
>    commit, so `58aaa00` stays reachable) and build it. Move persistence's `:21` to that sha and `:22`
>    to `utils` `5d14f56`, which is `utils`' `origin/master` and built; `JpaSession.dependentsOf` can
>    then hand its walk to `Graph.ancestors`. Push and build persistence. Move
>    `SkyBlock-Simplified/api`'s `:38`, `:39` and `:45`, merge its `feat/indexing` to `master` and build
>    `master-SNAPSHOT`, which data reaches through `:71`. Move github's `:37`, then push and build its
>    `feat/indexing`. Land the fix for "A consumer can still force a full SkyBlock rehydration" below,
>    so the sha skyblock publishes carries no public `getSessionManager()`, then move skyblock's `:35`,
>    `:38`, `:39` and `:42`, push and build it. Last, move hypixel's `:35`, `:38`, `:39` and `:42`, and
>    data's `:67`, `:76`, `:78` and `:79`.
> 2. The convention pins. Walk `toolsmith jitpack order collections` over the modules outside the
>    chain: the thirteen that pin `collections` `9696ca5` by convention only - `expression`, `image`,
>    `manager`, `reflection`, `scheduler`, `gson-extras`, `minecraft-text`, `yaml`, `client`,
>    `dataflow`, `mojang`, `asset-renderer` and `discord4j-framework` - and `spring-framework`, which
>    pins `client` and `gson-extras`. Each moves its `collections` pin, its `utils` pin where it has
>    one, and every pin on a module rebuilt before it, and is pushed and built. Ten of them pin `utils`
>    `3d8af56`; `nbt-factory`, which pins `utils` alone and falls outside that order, is the eleventh
>    and moves before `asset-renderer`, which pins it. `scheduler` finishes `fix/shutdown-hook` into
>    `master` before its `collections` pin moves, so the one sha it builds carries the hook's removal
>    as well.
> 3. The second pass. Each library step 2 rebuilt is a new sha the chain has to follow. persistence
>    moves `:23` `reflection`, `:24` `gson-extras` and `:25` `scheduler`, and is pushed and built again.
>    github follows `client` and `gson-extras`. Then `SkyBlock-Simplified/api`, skyblock, hypixel and
>    data, in that order, each move their persistence pin and every pin on a module step 2 or this
>    pass rebuilt; `SkyBlock-Simplified/api` lands on `master` again, so data's `master-SNAPSHOT`
>    follows it. bot and `SkyBlock-Simplified/server`, which pin modules from both sides, move last,
>    and bot's cannot be verified until it builds (below).
>
> - Affected: `build.gradle.kts:21-25`; `Simplified-Api/github/build.gradle.kts:35-37`;
>   `Simplified-Api/skyblock/build.gradle.kts:35`, `:38-42`, `:46`;
>   `Simplified-Api/hypixel/build.gradle.kts:35`, `:38-44`, `:47-48`;
>   `SkyBlock-Simplified/api/build.gradle.kts:35`, `:38-42`, `:45`;
>   `SkyBlock-Simplified/data/build.gradle.kts:45`, `:61-62`, `:67`, `:71`, `:76`, `:78-79`;
>   `Simplified-Dev/expression/build.gradle.kts:21-22`; `Simplified-Dev/image/build.gradle.kts:21-22`;
>   `Simplified-Dev/manager/build.gradle.kts:21`; `Simplified-Dev/reflection/build.gradle.kts:21-22`;
>   `Simplified-Dev/scheduler/build.gradle.kts:22`; `Simplified-Dev/gson-extras/build.gradle.kts:21-23`;
>   `Minecraft-Library/minecraft-text/build.gradle.kts:36-38`;
>   `Simplified-Dev/yaml/build.gradle.kts:21-23`;
>   `Simplified-Dev/client/build.gradle.kts:43-46`; `Simplified-Dev/dataflow/build.gradle.kts:36-39`;
>   `Simplified-Api/mojang/build.gradle.kts:35-39`, `:43`;
>   `Simplified-Dev/spring-framework/build.gradle.kts:40-41`;
>   `Minecraft-Library/asset-renderer/build.gradle.kts:174-179`, `:183`, `:189`, `:195`;
>   `Simplified-Dev/discord4j-framework/build.gradle.kts:43-49`;
>   `Minecraft-Library/nbt-factory/build.gradle.kts:38`;
>   `SkyBlock-Simplified/bot/build.gradle.kts:48-49`, `:51`;
>   `SkyBlock-Simplified/server/build.gradle.kts:42-44`, `:47-50`, `:53`;
>   `Simplified-Dev/collections` branch `feat/indexing` at `58aaa00`;
>   `Simplified-Dev/scheduler` branch `fix/shutdown-hook` at `4c401ab`
> - Type: **GAP**
> - Status: **OPEN** - needs the user's pushes and JitPack builds, `collections` first

> #### A queued corpus write skips the session's link check
> `JpaSession.write` links an upsert's rows against the rows the session holds before anything reaches
> the source, and refuses a row whose plain single-valued `@Linked` field carries no id or names no
> row. The one production writer of the corpus, data's `WriteQueueConsumer`, holds no session: it
> writes through the source `SkyBlockData.writing(...)` returns, so its upserts reach GitHub
> unchecked, and a delete of a row other rows still name is checked on no path. Such a write lands as
> a commit. A reading session whose tick finds the document moved then fails the rebuild that relinks
> the dangling row, and every type that rebuild covers stays `DEGRADED` on its previous generation and
> fails again at each tick; every `SkyBlockData.connect()` after the commit fails, corpus-wide. Both
> last until another commit repairs the data.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:218` -
>   `apply`; `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:130` -
>   `writing`; `src/main/java/dev/simplified/persistence/JpaSession.java:395` - `write`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java:270` - `resolveLinks`
> - Type: **RISK**
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
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:59` -
>   `sessionManager` and its generated getter, `:113` - `connect()`;
>   `SkyBlock-Simplified/bot/src/main/java/dev/sbs/bot/SimplifiedBot.java:48` - `main`;
>   `SkyBlock-Simplified/bot/src/test/java/dev/sbs/bot/TestLifecycleListener.java:19` -
>   `testPlanExecutionStarted`, `:26` - `testPlanExecutionFinished`;
>   `Simplified-Api/skyblock/src/test/java/api/simplified/skyblock/LocalSkyBlockData.java:93` -
>   `connect`, `:105` - `disconnect`;
>   `Simplified-Api/hypixel/src/test/java/api/simplified/hypixel/response/skyblock/stats/LocalSkyBlockData.java:110`
>   - `connect`, `:122` - `disconnect`
> - Type: **RISK**
> - Status: **OPEN**

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
