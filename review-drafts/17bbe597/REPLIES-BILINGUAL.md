# Replies to the review of #67855

## AarryaSaraf

In reply to [Aarrya’s comment](https://github.com/ray-project/ray/pull/67855#issuecomment-6671027378).

### English

Thanks, Aarrya. I have moved the pruning decision into listing. Partition substitution now uses the existing column-substitution visitor in a common helper, with the Parquet adapter checking partition values and schema. An existing pushed data filter no longer turns it off: listing applies that filter first, then intersects the remaining groups with those selected after partition substitution. Both filters stay in place.

Nested AND/OR/NOT, null checks and bounded literal IN are covered now. Narrow-integer IN and floating-point residual predicates still fall back; the description has a coverage table. The helper is format-independent, but I have kept admission in the source adapter: it must supply the values the reader actually returns, match the statistics to the reader’s field types, and preserve the empty-result schema. For example, MCAP keeps a message’s own `topic` even under a conflicting `topic=.../` directory, so a path value is not automatically a valid substitute. Only the built-in local Hive Parquet path has those checks here; support for other sources needs its own adapter and reader tests.

The part I would like your input on is the empty result. Keeping a data group just for its schema still caused an unnecessary read. Instead, listing emits at most one schema-only manifest entry per verified shard; the reader uses it to emit a typed empty table, with normal partition columns and projection, before opening data files. Since [#67037](https://github.com/ray-project/ray/pull/67037) also changes these shared interfaces, would you carry that instruction in the manifest?

Skipping a group also skips the work that could raise an error. A pushed expression such as division by zero can stop raising when the groups that would evaluate it are pruned. A task containing only schema-only entries does not construct the data scanner, so options checked there, such as `batch_size`, may not be validated either. I have kept that behavior without adding a separate validation pass or disabling the hint for those options. Errors from required footers and reads that actually run still propagate.

### 中文对照

谢谢 Aarrya。我已经把裁剪决定移到了 listing。分区替换现在放在通用 helper 中，使用已有的列替换 visitor，由 Parquet 适配器核对分区值和 schema。已有数据过滤下推也不再禁用它：listing 先应用该过滤，再与代入分区值后选出的行组取交集。两道过滤都保留。

现在覆盖了嵌套 AND／OR／NOT、null 检查和长度受限的字面量 IN。窄整数 IN、浮点 residual 谓词仍然回退，正文已用表格列出范围。Helper 不依赖格式，但是否启用仍由数据源适配器决定：它需要提供 reader 实际返回的分区值、核对统计与字段类型，并保留空结果 schema。例如 MCAP 会保留消息自身的 `topic`，即使它与 `topic=.../` 目录名不同，因此不能一律拿路径值代入。本次只为内建本地 Hive Parquet 建立了这些检查；其他数据源需要自己的适配及 reader 测试。

我想请你帮忙判断的是空结果的处理。只为取得 schema 而保留一个数据行组，仍然会产生不必要的读取。现在改为每个核对过布局的 shard 至多发出一个 schema-only manifest 条目；reader 在打开数据文件前，按正常的分区列处理和投影生成带类型的空表。[#67037](https://github.com/ray-project/ray/pull/67037) 也在调整这些共享接口，你会把这种指令放在 manifest 中吗？

错误行为取决于实际还执行哪些工作。会执行某个下推表达式的行组被裁掉后，例如其中会出现除零，可能就不再报错。只含 schema-only 条目的任务不构造数据 scanner，因此原本在那一步检查的参数，例如 `batch_size`，也可能不再校验。我保留了这种行为，没有单独增加校验，也不因这些参数禁用 hint。必需的 footer 读取及实际执行的读取中的错误仍然传递。

## Jadiouo

In reply to [Jadiouo’s review](https://github.com/ray-project/ray/pull/67855#pullrequestreview-5796666582).

### English

Thanks for checking the base too, Jadiouo. You caught the gap in the e2e test: it checked the answer but could not tell whether pruning happened. The new tests observe actual read workers and compare against the same code with the listing hint disabled.

For the original issue’s query, Arrow 17.0.0 and 23.0.1 both consumed 25 rather than 160 groups, from five rather than 32 files. The test checks the complete multiset of 5,000 `(x, year, month)` tuples, including duplicates, and the schema in both arms. Footer reads still happen; these are data-consumption counts, not total opens or bytes.

I moved the planning regressions into the listing rule’s test module, before its main guard. The focused run passed 306 tests on each Arrow version, including 21 schema-only end-to-end tests, with one existing pre-footer empty-input failure marked expected. Those checks ran before the latest upstream integration, with matching Core. The changed files are identical, but the full selection has not been rerun on the integrated revision, and full upstream CI has not run.

I also reran timings on the final integrated commit. In four pairs per case, the 3.735 GB selective query had a median time reduction of 14.10%, while the small all-kept query was slower in every pair, with a median increase of 5.91%. Caches were not cleared and the container reached its memory limit. The description includes all samples and these limits.

I have not rerun your exact 20-query set. If you still have the script, could you share it? The cases that fell back in your run would be useful to compare on the same input. Pruning is now in listing; the design question I raised with Aarrya is how the manifest should carry the empty-result schema without retaining a data group just for it.

### 中文对照

谢谢 Jadiouo 也检查了基线。你发现了端到端测试的缺口：它检查了答案，却不能判断裁剪有没有发生。新的测试观察真实读取 worker，并与同一份源码关闭 listing hint 的运行对照。

对原 issue 的查询，Arrow 17.0.0 和 23.0.1 下实际消费都从 160 个行组降至 25 个，从 32 个文件降至五个。测试比较两臂全部 5,000 个 `(x, year, month)` 元组的完整多重集，包含重复计数，并核对 schema。Footer 读取仍然发生；这些是数据消费计数，不是总打开次数或字节数。

我把计划回归移到了 listing 规则的测试模块中，放在 main guard 之前。两个 Arrow 版本的定向运行各通过 306 项，包括 21 项 schema-only 端到端测试，另有一个已知的 footer 前空输入用例标记为预期失败。这些检查运行于合入最新上游之前，使用匹配 Core。变更文件与整合版相同，但整合后尚未重跑全部定向测试，也未运行上游完整 CI。

我也在最终整合 commit 上重新计时，每个场景四对。3.735 GB 可裁查询的配对降时中位数为 14.10%，小规模全保留查询则四对全部变慢，中位增幅为 5.91%。没有清空缓存，容器也触及内存上限；正文保留了所有样本和这些限制。

你那组确切的 20 条查询还没有重新运行。如果脚本还在，能分享一下吗？你当时回退的那些情况，值得用同一份输入重新比较。裁剪现在已经移到 listing；我向 Aarrya 提出的设计问题是，manifest 应该怎样携带空结果的 schema，而不必仅为它保留一个数据行组。
