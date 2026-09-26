# Known open

Open items after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/), and the
ownership of the connect and hydrate path in [`notes/connection-flow/`](notes/connection-flow/).

> #### A queued corpus write skips the session's link check
> `JpaSession.write` checks an upsert against the rows the session holds before anything reaches the
> source, and refuses the whole write when a row's plain single-valued `@Linked` field carries no id
> or names no row; a list or `Optional` link tolerates a miss. It checks no delete. data's
> `WriteQueueConsumer`, the one production caller of `SkyBlockData.writing(...)`, holds no session
> and writes through that source, so its upserts reach GitHub unchecked, and a delete of a row other
> rows still name is checked on no path. Each layer such a write changes lands as a commit, which the
> consumer counts as a success.
>
> A process whose first `SkyBlockData.connect()` comes after that commit fails to connect,
> corpus-wide, since a connect reads every layer at the branch tip. A running session reads the
> changed layer only once the catalogue is regenerated in a commit of its own, because a document's
> fingerprint is the hash the catalogue records, or once a rebuild of another type covers it. That
> rebuild fails the link pass on the dangling row and leaves every type it covers `DEGRADED` on its
> previous generation, to be re-read and fail again at each tick. Both last until another commit
> repairs the data. The skyblock models declare twelve plain single-valued links, `Item.category`,
> `Accessory.item` and `Mixin.item` among them. No maintained module puts a write on the queue
> outside tests.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:218` -
>   `apply`; `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:152` -
>   `writing`; `src/main/java/dev/simplified/persistence/JpaSession.java:401` - `write`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java:270` - `resolveLinks`
> - Type: **GAP**
> - Status: **OPEN**

> #### Repairing the client's response cache makes a corpus write's rebuild read from before the write
> `JpaSession.write` rebuilds the written type once the write lands. Over the corpus's read-write
> source that rebuild asks GitHub for the branch tip, holds the catalogue at the commit the write
> made and reads every layer there, so it publishes the written rows. It does because the client's
> `ResponseCache` retains no response: `store` creates each URL's bucket as an empty map and adds the
> variant to it afterwards, and Caffeine fixes the bucket's lifetime at creation from
> `ResponseCacheExpiry.expireAfterCreate`, which answers zero for a bucket holding no variant and
> never sees the variant added in place. Every GET reaches GitHub, and no conditional request is
> made. No client test stores through `store` and looks the entry up.
>
> `GitHubCorpus` reads through one client and writes through another, each with its own cache, and a
> write clears nothing the read client holds. Once `store` retains a response for the `max-age`
> GitHub sends, the read client answers the branch tip, and a file read at the branch, from before the
> corpus's own write for up to that long. The writer's rebuild then republishes the pre-write rows as
> `CURRENT` until the written type's next tick; its next write routes by the layers before the write,
> so a delete of a row the previous write added edits nothing and returns; and `CorpusOrigin.edit`
> sends the sha of the body the previous edit replaced, which GitHub refuses. A repair of `store` that
> does not also clear the read client's cache on a corpus write brings all three.
>
> - Affected: `Simplified-Dev/client/src/main/java/dev/simplified/client/cache/ResponseCache.java:277` -
>   `store`; `Simplified-Dev/client/src/main/java/dev/simplified/client/cache/ResponseCacheExpiry.java:48` -
>   `expireAfterCreate`; `Simplified-Api/github/src/main/java/api/simplified/github/GitHubCorpus.java:237` -
>   `write`, `:341` - `poll`, `:199` - `blob`;
>   `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/CorpusOrigin.java:110` -
>   `refreshedLayersOf`, `:184` - `edit`
> - Type: **RISK**
> - Status: **OPEN**

> #### A rescheduled queue write re-sends its row, and can revert a later write
> data's `WriteQueueConsumer` puts a failed write back on its retry map with the row it was enqueued
> with, and a retry writes that row again whatever has landed since. The source replaces a written row
> whole, so a later write to the same row that lands while the first waits out its backoff, up to 31
> minutes at the configured five attempts, is reverted when the retry lands: a retried upsert restores
> a row a later delete removed, and a retried delete removes one a later upsert wrote. Retries that
> fall due in the same drain, as every elapsed one does on the first scan after a restart, are taken
> in the retry map's order rather than the order they were enqueued in, so two retries of one row can
> land oldest last.
>
> Within one drain the one fresh envelope the cycle polled is applied first and every due retry after
> it. A retry of the same row and operation is in the same request, where the source keys rows with
> the later one winning, so the fresh row is never written, yet its envelope is counted as written. A
> retry of the other operation is a later request, and undoes the fresh one.
>
> A write GitHub committed but whose answer is lost, such as a timeout reading the answer to the PUT,
> throws like one that never landed: Feign's default retryer re-sends the PUT under the blob sha it
> read, and GitHub refuses it as stale. A write that edits two layers and fails on the second throws
> the same way after the first has committed. Either is counted as a failure and retried whole,
> re-sending rows already on the branch and reverting any write to them that landed in between. No
> maintained module puts a write on the queue outside tests.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:182` -
>   `cycle`, `:218` - `apply`, `:265` - `reschedule`;
>   `src/main/java/dev/simplified/persistence/source/DocumentSource.java:285` - `ReadWrite.write`
> - Type: **RISK**
> - Status: **OPEN**
