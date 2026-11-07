# Replies to the review of #67855

## AarryaSaraf

In reply to [Aarrya’s comment](https://github.com/ray-project/ray/pull/67855#issuecomment-6671027378).

### English

Thanks, Aarrya. I have prepared a [listing-side revision](https://github.com/m4ru1/ray/commit/eb6727009e5305106f45607cffd549e4ca86e512). The reader no longer makes the pruning decision. Partition substitution now uses the existing column-substitution visitor in a common helper, with a Parquet adapter checking the partition values and schema. An existing pushed data filter no longer turns this off: listing applies it first, then intersects the remaining groups with those selected by the partition-substituted filter. The original filters stay in place.

The expression handling now includes nested AND/OR/NOT, null checks and bounded literal IN, rather than just the original comparison patterns. I have not made it accept every expression yet. For example, int64 IN is covered, but narrow-integer IN and floating-point predicates still fall back. The adapter also remains limited to built-in local Hive Parquet, where its partition values and output schema can be checked against the reader.

One issue that came up while moving this was preserving the schema when no data survives. Keeping a data group just for its schema still caused an unnecessary read. Instead, listing now emits at most one schema-only manifest entry per verified shard, alongside any data chunks. The paired reader emits the corresponding empty table before opening data files, with its normal partition columns and projection. This also handles a pushed filter that turns out to return zero rows at execution time.

The same revision also has a small-query cost: in two same-machine warm-cache pairs against clean `891a5a8`, the 205,457-byte all-kept case added 157.75/167.13 ms. That tradeoff is included in the description alongside the selective result.

Since you are also changing the manifest/count interfaces in [#67037](https://github.com/ray-project/ray/pull/67037), would you put that reader instruction in the shared manifest? It avoids changing the general executor, but introduces an entry that represents schema rather than data to scan. I would like to settle that choice before widening the source support.

### 中文对照

谢谢 Aarrya。我准备了一版[在 listing 阶段裁剪的修改](https://github.com/m4ru1/ray/commit/eb6727009e5305106f45607cffd549e4ca86e512)。Reader 不再决定是否裁剪。分区替换现在放在通用 helper 中，使用已有的列替换 visitor，由 Parquet 适配器核对分区值和 schema。已有数据过滤下推也不再让这项优化失效：listing 先应用该过滤，再将剩余行组与代入分区值后的过滤选出的行组取交集。原来的过滤条件都保留。

表达式处理现在覆盖了嵌套 AND／OR／NOT、null 检查和长度受限的字面量 IN，而不只是最初的几种比较模式。我还没有把它放开到所有表达式。例如，int64 IN 已覆盖，但窄整数 IN 和浮点谓词仍然回退。适配器也仍限于内建本地 Hive Parquet，这样才能按照 reader 的实际行为核对分区值和输出 schema。

移动这部分逻辑时遇到一个问题：没有数据留下时，如何保留 schema。仅为取得 schema 而保留一个数据行组，仍然会产生不必要的读取。现在改为对每个核对过布局的 shard 至多发出一个 schema-only manifest 条目，与数据 chunks 一起传递。配对 reader 在打开数据文件前，按正常的分区列处理和投影生成相应空表。这也处理了下推过滤直到执行时才返回零行的情况。

同一修订也存在小查询成本：与干净的 `891a5a8` 在同一台机器上做两次暖缓存配对，205,457-byte 全保留场景增加了 157.75／167.13 ms。正文同时列出了这一取舍和可裁场景的结果。

你也在 [#67037](https://github.com/ray-project/ray/pull/67037) 中调整 manifest／count 接口，你会把这种给 reader 的指令放在共享 manifest 中吗？它避免了修改通用执行器，但也引入了一个代表 schema、而非待扫描数据的条目。在扩大数据源支持之前，我想先把这个选择讨论清楚。

## Jadiouo

In reply to [Jadiouo’s review](https://github.com/ray-project/ray/pull/67855#pullrequestreview-5796666582).

### English

Thanks for running this against the base too, Jadiouo. That exposed the gap in the e2e test: it checked the answer, so it could pass even if the optimization did nothing. The new real-worker tests assert the exact row groups and files whose data is consumed, with pruning-disabled controls, as well as the complete rows and schema.

For the original issue’s 32 files with five groups each, the revised path consumes 25 rather than 160 groups, from five rather than 32 files, and returns the same 5,000 rows and schema. This holds on both Arrow 17.0.0 and 23.0.1. Those are data-consumption counts, not total opens or bytes; the footer reads still happen. All 26 related Bazel targets passed locally on both versions with usage explicitly disabled. The pre-footer empty-input case remains an expected failure.

A separate same-machine test of `eb672700` against clean `891a5a8` used 59.75 GB of local Parquet files. It consumed 440 rather than 3,520 groups, still reading data from all 55 files. The two timing pairs were 290.25/249.00 s upstream versus 9.89/9.30 s for this revision. Caches were not flushed, and adjacent upstream controls drifted by 20.4%, so the description limits that result to this workload; it also reports the 157.75/167.13 ms small-query overhead. Opens were not measured.

I also moved the planning regressions into the listing rule’s test module, before the main guard. Nested boolean expressions, null checks and bounded IN now have coverage within the supported type/source restrictions. I have not rerun your exact 20-query set, though. If you still have that script, could you share it? The cases that fell back in your run would be useful to compare on the same input, rather than treating the new tests as a reproduction of yours.

I agree with settling the placement first. The [revision](https://github.com/m4ru1/ray/commit/eb6727009e5305106f45607cffd549e4ca86e512) moves pruning into listing; the remaining design question I raised with Aarrya is how the manifest should carry enough information for the reader to preserve an empty result’s schema without keeping a data group just for that purpose.

### 中文对照

谢谢 Jadiouo 也在基线上跑了测试。这暴露了端到端测试的缺口：它只检查答案，所以优化完全不起作用时也可能通过。新的真实 worker 测试除了检查完整结果行和 schema，还精确断言实际消费的行组以及哪些文件的数据被消费，并设置关闭裁剪的对照。

对原 issue 的 32 个文件、每文件五个行组，修订后的路径从 160 个行组减少到 25 个，从消费 32 个文件的数据减少到五个，返回相同的 5,000 行和 schema。Arrow 17.0.0 和 23.0.1 下均得到这个结果。这些是数据消费计数，不是总打开次数或字节数；footer 读取仍然发生。两个版本下的全部 26 个相关 Bazel 目标均在显式关闭 usage 的本地环境中通过。Footer 之前即为空输入的用例仍是预期失败。

另一次同机实验将 `eb672700` 与干净的 `891a5a8` 对照，使用 59.75 GB 本地 Parquet 文件。实际消费从 3,520 组减至 440 组，但双方仍读取全部 55 个文件的数据。两对耗时为上游 290.25／249.00 秒、本修订 9.89／9.30 秒。实验未清缓存，相邻上游控制运行漂移 20.4%，所以正文将这个结果限定在本次负载，也保留了小查询增加 157.75／167.13 ms 的开销。本轮没有测量 opens。

我也把计划回归移到了 listing 规则的测试模块中，放在 main guard 之前。嵌套布尔表达式、null 检查和长度受限的 IN 已在支持的类型／数据源范围内有覆盖。不过，你那组确切的 20 条查询还没有重新运行。如果脚本还在，能分享一下吗？你当时回退的那些情况，值得在同一份输入上重新比较，而不能把新增测试当作已经复现了你的检查。

我赞同先把处理位置确定下来。[这次修改](https://github.com/m4ru1/ray/commit/eb6727009e5305106f45607cffd549e4ca86e512) 已将裁剪移到 listing；我向 Aarrya 提出的剩余设计问题是，manifest 应该如何携带足够的信息，让 reader 在不为 schema 而保留数据行组的情况下，仍能保留空结果的 schema。
