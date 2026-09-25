# Known open

Open items after the document/database unification. Each stays here until it is
closed or accepted; the design itself is in [`notes/jpa-unification/`](notes/jpa-unification/), and the
ownership of the connect and hydrate path in [`notes/connection-flow/`](notes/connection-flow/).

> #### The connect check does not see getters, embeddables or `@Any`
> A session refuses to connect when a registered type, or an unregistered type its eager `@ManyToOne`
> and `@OneToOne` fields reach, declares an association a held generation cannot follow. The check
> reads fields only, and names only `@OneToMany`, `@ManyToMany`, `@ElementCollection` and a lazy
> `@ManyToOne` or `@OneToOne`. A type mapped through property-access getters carries its
> association annotations on the getters, which the check never reads, so it neither refuses a lazy
> association there nor follows an eager one. The fields inside an `@Embedded` component are not
> read, so a lazy association there is not refused and an eager one is not followed. Hibernate's
> `@Any` and `@ManyToAny` are not among the annotations it names. Any of the three on a registered
> type, or on a type it reaches, can put an uninitialized proxy or collection into a held
> generation, which throws `LazyInitializationException` once the read that loaded it has closed.
> No model in the workspace declares `@Embedded`, `@Access`, `@Any` or `@ManyToAny`.
>
> - Affected: `src/main/java/dev/simplified/persistence/JpaSession.java:597` - `refuseUnfollowable`
> - Type: **GAP**
> - Status: **OPEN**

> #### A queued corpus write skips the session's link check
> `JpaSession.write` links an upsert's rows against the rows the session holds before anything reaches
> the source, and refuses a row whose plain single-valued `@Linked` field carries no id or names no
> row. The one production writer of the corpus, data's `WriteQueueConsumer`, holds no session: it
> writes through the source `SkyBlockData.writing(...)` returns, so its upserts reach GitHub
> unchecked, and a delete of a row other rows still name is checked on no path. Such a write lands as
> a commit. A reading session whose tick finds the document moved then fails the rebuild that relinks
> the dangling row, and every type that rebuild covers stays `DEGRADED` on its previous generation and
> fails again at each tick; a process whose first `SkyBlockData.connect()` comes after the commit
> fails to connect, corpus-wide. Both last until another commit repairs the data.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:218` -
>   `apply`; `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/SkyBlockData.java:152` -
>   `writing`; `src/main/java/dev/simplified/persistence/JpaSession.java:395` - `write`;
>   `src/main/java/dev/simplified/persistence/JpaRepository.java:270` - `resolveLinks`
> - Type: **RISK**
> - Status: **OPEN**

> #### A corpus write's own rebuild reads the document from before the write
> `JpaSession.write` rebuilds the written type once the write lands. Over the corpus's writing
> origin that rebuild reads the rows from before the write. `CorpusOrigin.Writing.layersOf` polls the
> branch tip before the write edits the document, and again when the rebuild resolves its layers,
> and the second tip request is answered from the client's response cache, which keeps GitHub's
> answer for its `max-age` of a minute. As far as the corpus can tell the branch has not moved, so
> the held catalogue stays at the commit before the write and `CorpusOrigin.read` reads every layer
> at that commit. The rebuild republishes the pre-write rows, and every type it covers is relinked
> to them.
>
> It heals at the written type's next due tick. The rebuild forgets the fingerprint each covered type
> was read under, so that tick reads the type again whatever its fingerprint says, and by then the
> cached tip has expired. Until then the writing session serves the rows from before its own write -
> for the corpus's ten-minute cadence, between ten and twenty minutes, since the rebuild's
> publication restarts the cadence - and a type with no cadence of its own keeps them until a later
> rebuild covers it or the session connects again. No deployment in the workspace both writes and
> reads the corpus: `SkyBlockData.connect()` reads through a source with no write half, and data's
> `WriteQueueConsumer` writes through the source `SkyBlockData.writing(...)` returns with no session
> over it. The path skyblock's README documents for a token-holding caller - connecting
> `new JpaConfig(..., SkyBlockData.writing(corpus))` on its own `SessionManager` and writing through
> that session - is exactly the one this affects.
>
> - Affected: `Simplified-Api/skyblock/src/main/java/api/simplified/skyblock/CorpusOrigin.java:149` -
>   `Writing.layersOf`, `:57` - `read`;
>   `Simplified-Api/github/src/main/java/api/simplified/github/GitHubCorpus.java:222` - `tip`,
>   `:279` - `poll`; `src/main/java/dev/simplified/persistence/JpaSession.java:395` - `write`
> - Type: **RISK**
> - Status: **OPEN**

> #### A rescheduled queue write re-sends its rows, and can revert a later write
> data's `WriteQueueConsumer` puts a failed write back on its retry map with the rows it was enqueued
> with, and a retry writes those rows again whatever has landed since. A later write to the same row
> that lands while the first waits out its backoff is reverted when the retry lands. Within one drain
> every ready retry is applied after the one fresh envelope the cycle polled, and the source keys a
> request's rows with the later one winning, so a retry of a row lands over a fresher write to it in
> the same drain as well.
>
> A write whose response is lost after GitHub committed it, such as a timeout reading the answer to
> the PUT, throws like one that never landed. It is counted as a failure and retried as one,
> re-sending rows that are already on the branch, which reverts any write to the same rows that
> landed in between.
>
> - Affected: `SkyBlock-Simplified/data/src/main/java/dev/sbs/data/write/WriteQueueConsumer.java:182` -
>   `cycle`, `:218` - `apply`, `:265` - `reschedule`;
>   `src/main/java/dev/simplified/persistence/source/DocumentSource.java:167` - `Writable.write`
> - Type: **RISK**
> - Status: **OPEN**
