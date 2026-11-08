# [Data] Prune mixed partition/data filters during Parquet listing

This revision moves mixed partition/data pruning into Parquet listing, following [Aarrya’s suggestion](https://github.com/ray-project/ray/pull/67855#issuecomment-6671027378). For the query in [#67567](https://github.com/ray-project/ray/issues/67567), read workers receive only the row groups that may match, while the complete row filter stays in place.

Before this PR, a mixed filter such as `(year == "2023") | (x < 1000)` leaves all row groups to be read. The original PR made the pruning decision inside the reader. This revision makes it while reading footers: substitute each file’s partition values, then intersect the selected groups with those allowed by any existing pushed data filter. For that query, files under `year=2023` keep all groups; for other years, the footer can exclude groups using `x < 1000`.

Subsystems: **Logical planning / query optimization** and **IO** (Datasource V2 listing, manifests, packing and readers).

```mermaid
flowchart TB
  subgraph Before["Before: upstream mixed filter"]
    subgraph BIO["IO: Datasource V2"]
      BL["FooterFileIndexer: list all mixed-filter candidates"]
      BR["ParquetFileReader: read listed groups"]
      BL --> BR
    end
    subgraph BP["Logical planning / query optimization"]
      BF["Filter: evaluate mixed expression after the read"]
    end
    BR --> BF
  end
  subgraph After["After: this revision"]
    subgraph AP["Logical planning / query optimization"]
      H["DeriveListFilesPushdown: attach residual hint"]
      F["Retained Filter: evaluate complete expression"]
    end
    subgraph AIO["IO: Datasource V2"]
      L["FooterReader: substitute partitions and intersect groups"]
      S["Schema-only entry: zero chunk read budget"]
      M["FileManifest / OnlineBinPacker: preserve both entry types"]
      R["FileReader: emit typed empty table, then read retained chunks"]
      L --> M --> R
      L --> S --> M
    end
    H --> L
    R --> F
  end
  classDef changed fill:#fff9c4,stroke:#f9a825,color:#000
  classDef added fill:#c8e6c9,stroke:#2e7d32,color:#000
  class H,L,M,R changed
  class S added
```

## Changes after review

The partition substitution now uses Ray’s existing column-substitution visitor in a common helper. The Parquet adapter checks the partition values and file schema. An existing pushed data filter no longer disables the optimization; both that filter and the complete residual filter remain in place. The supported expressions now include nested AND/OR/NOT, null checks and bounded literal IN, rather than only the original comparison patterns.

Preserving an empty result’s schema needed a separate change. Keeping a data group just for its schema still caused a read. Listing now emits at most one schema-only manifest entry per shard with a verified layout, alongside any surviving data chunks. The paired reader uses its normal partition-column handling and projection to emit a typed empty table before opening data files. It does this even when data chunks follow, so the schema also survives when filtering returns no rows at execution time. The entry retains the real source file size, has a zero chunk read budget, and is not an exact residual-filter count.

Aarrya, since [#67037](https://github.com/ray-project/ray/pull/67037) also changes the shared manifest/count interfaces, would you put this reader instruction in the manifest? It avoids changing the general executor, but adds an entry that represents schema rather than data to scan. I would like to settle that choice before extending source support.

## Scope and error behavior

The new path is limited to a root `Filter → ReadFiles → ListFiles` query using built-in local Hive Parquet. Here, “fallback” means declining the new residual hint; existing pushdowns and the full row filter still run. Support for an expression does not guarantee that its statistics will eliminate a group.

### Expression coverage

These restrictions apply to the residual expression used for the new hint, not to an already-pushed data filter. The residual must reference a partition column, and every part of it must be supported.

| Expression | Accepted by the hint | Falls back |
|---|---|---|
| Signed-integer comparisons | `==`, `!=`, `<`, `<=`, `>`, `>=` on `int8`/`int16`/`int32`/`int64`; literals must fit the field width | Out-of-range literals, such as comparing an `int8` field with `128` |
| String/bool comparisons | `==` and `!=` | Ordering comparisons such as `name < "m"` |
| Boolean composition and null checks | Nested AND/OR/NOT; `is_null` and `is_not_null` on a column or supported literal | An unsupported branch declines the whole hint, even if another branch could simplify to true |
| Literal IN | 1–256 non-null values of one type, matching an `int64`, bool or string input | Narrow-integer IN, mixed types, nulls, empty lists or lists longer than 256 |
| Column-to-column comparisons | A type-compatible comparison whose partition operand becomes a typed literal, for example `x < partition_limit` | Two column operands remain after substitution |
| Floating point, arithmetic and UDFs | `float64` may be an unreferenced payload column | Floating-point operands, arithmetic or UDFs in the residual |

Missing statistics can keep every group, and IN pruning depends on the Arrow version. This does not include [#67943](https://github.com/ray-project/ray/pull/67943)’s derived arithmetic or prefix predicates.

### Supporting other data sources

I separated the reusable expression work from the source-specific checks. The common helper takes partition values that have already been converted to the reader’s types, then uses the existing column-substitution visitor. It does not parse paths, read statistics or decide the output schema. The Parquet adapter has to establish three things before using its result:

- **The substituted value must be the value the reader returns.** This adapter uses the reader’s Hive parser and type conversion. Other readers can give the same path a different meaning: [MCAP keeps a message’s own `topic`](https://github.com/m4ru1/ray/blob/17bbe5978dfb326746fca975be4a3abb9277a6b7/python/ray/data/_internal/datasource_v2/formats/mcap/mcap_reader.py#L247-L272), so a message with `topic="lidar"` under `topic=camera/` must not be treated as `topic="camera"`. Parsing the path correctly is not enough to justify that substitution.
- **The source needs a way to exclude its own read units.** Parquet checks physical field types against the fixed reader schema, then uses footer statistics to select row groups. A physical `INT32` alone is insufficient: it might represent an integer, date or decimal. ORC already shares the Arrow scanner and partition parser, but its current indexer lists whole files and its [reader does not implement native stripe pruning](https://github.com/m4ru1/ray/blob/17bbe5978dfb326746fca975be4a3abb9277a6b7/python/ray/data/_internal/datasource_v2/formats/orc/orc_file_reader.py#L34-L40). Reusing substitution alone would not supply that missing selection step.
- **Removing every data group must still leave the right schema.** The schema-only consumer is in the common `FileReader`, which ORC also inherits. It uses the normal partition-column insertion and projection, but relies on the producer having checked the layout; only the Parquet path produces those verified entries here. Another reader’s extra fields, conversions or schema evolution need corresponding checks. Keeping a data group solely for its schema would avoid this new agreement, but would also retain an unnecessary read.

For now I enable that combination only for the built-in local Hive Parquet scanner and indexer. Remote filesystems and subclasses are excluded because this PR has not established the same contract for them, not because partition substitution inherently requires local storage. Other indexers can implement `FileIndexer.with_residual_pruning`; its default leaves listing unchanged. An implementation needs to establish its reader contract and add real-reader tests for values, empty schemas and errors.

| Area | Enabled here | Existing path retained |
|---|---|---|
| Source | Built-in Hive Parquet on the local filesystem, including its normal retry wrapper | Remote/custom filesystems, non-Hive partitioning and scanner/indexer subclasses |
| Schema and reader configuration | Matching fixed primitive logical/physical schema and normal projection | Unsupported types, nested/extension types, schema/field metadata, incompatible footer layout, synthesized columns or custom Parquet format options |
| Query/execution | Root `Filter → ReadFiles → ListFiles`, unordered and unshuffled | Downstream maps, count/limit plans, ordered/shuffled reads, checkpointing or tolerated block errors |

### Error behavior

Required footer reads still happen, and errors on the read path that actually runs propagate normally. Pruned groups are not read or evaluated just to reproduce an error: a corrupt page or a division-by-zero expression confined to those groups may no longer raise.

A read task containing only schema-only entries returns without constructing the data scanner. Options normally checked at that step, such as an invalid `batch_size`, may therefore go unchecked when no data needs to be read. There is no separate validation pass and no `batch_size` gate on the listing hint. If data chunks remain, the ordinary reader path still runs. This is a change from the original PR’s decision to disable pruning whenever a data filter was already pushed; it does not promise identical errors for work that pruning removes.

## Tests

Thanks, [Jadiouo](https://github.com/ray-project/ray/pull/67855#pullrequestreview-5796666582), for checking the base too. The old e2e test could pass while reading everything. The tests now observe real read workers, use pruning-disabled controls, and check consumed groups, output and schema. The planning regressions are in the listing rule’s test module, before its main guard.

The original issue’s regression compares the complete `(x, year, month)` multiset in both arms, including duplicate counts, and checks the schema. On PyArrow 17.0.0 and 23.0.1, it returned the same 5,000 fully checked rows while consumption fell from 160 to 25 row groups and from 32 to five data files. Footer reads still happen; these are data-consumption counts, not total opens or bytes.

The focused unit, real-worker and footer tests passed **306 tests with one expected failure on each Arrow version**. The expected failure is the known case where partition pruning removes every file before footer listing, so no schema entry reaches the reader. Jadiouo’s original 20-query script has not been rerun as a set.

The functional runs used the pre-integration revision with matching Ray Core. Its changed source and test files are identical to the final integration; the remaining difference in the checked Data source is an unrelated upstream docstring. The full 306-test selection has not been rerun after that integration. The performance runs below did use the final integrated revision and its matching Core. These local checks are not a full upstream CI pass.

The original-issue regression can be run with:

```bash
python -m pytest \
  'python/ray/data/tests/datasource_v2/test_schema_only_manifest.py::test_empty_output_schema_and_actual_reads[original]'
```

## Performance

I reran the comparison on final commit `17bbe597` against clean upstream `aa00fdbb`, with matching Ray Core and PyArrow 23.0.1.

On the 3.735 GB selective query, the revision read 440 row groups instead of 3,520 and returned the same 450,560 rows. Across four pairs, query time fell by 8.50–27.25%, with a median reduction of 14.10%. Both versions still read data from all 55 files.

I also checked a small query that keeps every row, so the extra listing work cannot save a read. That query was slower with the revision in **all four pairs**: an extra 86.9–335.6 ms, or 2.35–9.60%, with a median increase of 5.91%.

These were local runs with a pre-read, without clearing the cache. The container reached its 5 GiB memory limit and reclaimed memory, though no OOM or spill was observed. Upstream timings for the selective query varied from 6.779 to 9.061 s, so four pairs are not enough to tell how repeatable the size of the gain is. Cold-storage and remote-filesystem performance remain untested.

<details>
<summary>Workloads, all timing pairs and setup</summary>

The selective dataset has 55 Hive-partitioned files, each with 64 groups of 1,024 rows. Each row carries a 1,024-byte ASCII string; the files are uncompressed, use no dictionary encoding, and have statistics only for `x`. They occupy 3,735,281,660 bytes. The filter `(p == -1) | (x >= 57344)` keeps the last eight groups in each file.

The small control has 32 files, five groups per file and 205,457 bytes in total. Its filter, `(year == "2023") | (x >= 0)`, keeps all 32,000 rows and 160 groups. The revision’s hint is active in this case too.

I alternated the order of the two versions. All four pairs for each query are below.

Selective query:

| Pair order | Upstream | This revision | Query-time change |
|---|---:|---:|---:|
| Upstream → revision | 9.0607 s | 6.5920 s | −27.25% |
| Revision → upstream | 8.7438 s | 7.0726 s | −19.11% |
| Upstream → revision | 6.7789 s | 6.2025 s | −8.50% |
| Revision → upstream | 6.9334 s | 6.3033 s | −9.09% |

Small all-kept control:

| Pair order | Upstream | This revision | Query-time change |
|---|---:|---:|---:|
| Upstream → revision | 3.7024 s | 3.7893 s | +86.9 ms (+2.35%) |
| Revision → upstream | 3.5155 s | 3.7555 s | +240.1 ms (+6.83%) |
| Upstream → revision | 3.4964 s | 3.8320 s | +335.6 ms (+9.60%) |
| Revision → upstream | 3.5988 s | 3.7785 s | +179.8 ms (+4.99%) |

The runs used Linux ARM64 Docker on a 32 GiB Mac, with Python 3.11.18. The container had four CPUs and 5 GiB of memory; Ray used two CPUs, a 1 GiB object store and a 512 MiB Data soft budget. Reads were unordered, with spilling and usage statistics disabled.

I checked the complete output, including payloads, schema and duplicate counts, and observed row-group reads in four separate runs before timing. Each timing run used a full sequential pre-read followed by a fresh Ray instance. I then timed from building the Dataset/filter until all results had been consumed, using 4,096-row Arrow batches without prefetch. Those timings include the remaining row-count, schema and null checks, but not the pre-read or Ray startup.

The reduction in consumed groups was observed directly. I did not measure the exact bytes or file opens saved, or how much of the pre-read data remained cached during each query.

</details>

This continues #67567’s proposed per-file substitution in the existing PR. It is distinct from #67943’s derived predicates and #67037’s manifest/count changes; their combined behavior has not been tested.

AI assistance was used.
