# Lance 14 全文检索语义与 lance-spark 执行路径（#3279 预研，驱动 #3263）

日期：2026-11-25；卡片：#3279（父卡 #3262）。只读调研，不提交集群作业、不改产品代码。
基线：`dev` 分支 `5cbafef`（worktree `sync-091`）；connector `0.9.1-beta.1`、Lance Core `14.0.3`、lance-namespace `0.12.0`（`pom.xml:55-58`）。
本文与早前的 [dev 检索优化分析](../analysis/lance-spark-search-optimization-analysis.md) 和 [10M 实施计划](search-optimization-10m-plan.md) 互补：那两份给方向与测量合同，本文给语义与代码级证据，并把 #3263 拆成可实施、可回退的改动。

**证据标注约定**（每条结论都可按编号复核）：

- `L:` 本仓库 `lance-spark-base_2.12/src/main/...` 的行号（dev `5cbafef`，与本次 worktree 同源）。
- `T:` 本仓库测试源码行号。
- `J:` `lance-core-14.0.3.jar` 的 `javap` 签名/字段；jar 路径 `~/.m2/repository/org/lance/lance-core/14.0.3/lance-core-14.0.3.jar`。本地没有 sources jar，本次从 Maven Central 只读下载了同版本 `lance-core-14.0.3-sources.jar`（sha256 `b194810527593f7d1987e485b961f41cac287a5984d2fe2f1b76fe0d26050436`），仅用于读 Java 层 javadoc。
- `R:` Lance 上游 `v14.0.3` 标签（commit `c7dc014f1e722ad7dcc9a731991e4f33bb8852bb`）的 Rust 源码，只读拉取到 `/tmp/lance-src`，用来补足 JNI 之下、Java API 看不到的 native 语义；未运行任何集群或本地 Spark 作业。
- 凡是标“推测”的，都是代码与文档都不能闭合、必须靠 #3263 的离线用例或集群专项证实的。

## 1. 结论摘要

1. dev 上 FTS 有三条 SQL 谓词入口——`lance_match`、`lance_match_phrase`、`lance_multi_match`——加两条表函数 `SEARCH` 与 `HYBRID_SEARCH`；没有专用 DataFrame API。谓词通过目录的 `FunctionCatalog` 注册、由 optimizer 规则搬进 relation options，表函数由 session extension 注册。两者最终都落到同一个“读 options 里的 fullTextQuery”的扫描路径（`L:java/org/lance/spark/read/LanceScanBuilder.java:157-160`）。
2. Lance 14 的 FTS 是单列 BM25（k1=1.2、b=0.75、`idf=ln((N-df+0.5)/(df+0.5)+1)`）；`_score` 是 nullable Float32。带 `limit` 时走 WAND 有界 top-k（`limit=usize::MAX` 表示全命中），不带时返回全部命中；tie 只按分数比较，不保证稳定（`R:rust/lance-index/src/scalar/inverted/builder.rs:1975-2010`）。
3. 分布式 rank 合并有两条完全不同的路：local 是“一 fragment 一 Spark task”，每个 task 打开一次 dataset、对该 fragment 建 scanner，limit 只在 Spark 下推 LIMIT 时进入，否则全部命中回传、Spark 全局排序/过滤；namespace 是单 partition 的 `queryTable`，服务端一次性算全局 top-k（`L:.../read/LanceSplit.java:92-103`、`L:.../read/LanceScanBuilder.java:616-660`、`L:.../search/LanceSearchScan.java:39-53`）。
4. 只要存在 Spark 侧 residual（典型 `_score > 0`、`MOD(id,2)=1`），namespace 路径就把 `k` 设为 `Integer.MAX_VALUE`，reader 再把整个响应当成一个 `byte[]` 解码；10M 规模、常见词、宽表时这是单 task 的堆内存与 IPC 放大点，而不是“慢一点”（`L:.../read/LanceScanBuilder.java:632-634`、`L:.../search/LanceSearchColumnarPartitionReader.java:80-95`）。
5. 当前没有 recall 缺口：Spark 在 `pushTopN` 拒绝时保留自己的全局 Sort/Limit，local 路径不会拿“每分片 top-k”当全局结果；FTS 的 `pushTopN` 只在 namespace 单分片且无 residual 时开启（`L:.../read/LanceScanBuilder.java:734-748`、`T:.../read/BaseFtsScoreColumnTest.java:186-237`）。真正的隐患是 tie 不稳定与 partial index 下 score 不可比，它们会让“offset 分页/翻页”漏行或重复（见 §4.3）。
6. 观测上，local 路径有 12 个 Scan 自定义指标，但 `ScanStats.indexCacheHits/Misses` 和 `allCounts/allTimes` 被 tracker 丢弃；namespace 路径（`LanceSearchScan`）没有 custom metrics、没有 `getMetaData`、没有 statistics，`QueryTableResponse.context` 也被忽略（`L:.../read/metric/LanceReadMetricsTracker.java:206-216`、`L:.../search/LanceSearchScan.java:24-53`、`J:org.lance.namespace.model.QueryTableResponse`）。
7. 测试覆盖了 match/phrase/advanced options/score/catalog-only fallback，local 与 namespace 双后端都有；缺口集中在 tie 稳定性、分页/offset、partial index、多列打分一致性、超大 k/内存边界和观测。文档对 BM25 公式、multi_match 的 max 语义、combined_fields/BM25F、document granularity、tokenizer 全量选项均未覆盖（§6）。
8. #3263 建议双轨：先做严格等价的过滤下推（把 residual 压到最小），再对 namespace 做“固定 snapshot + 稳定全序 + continuation 分批”的有界读取；local 路径保留 Spark 全局排序，候选上限只做显式 hint 且默认关闭。前置必须确认的三件事见 §7。

## 2. 入口与调用链（问题 1）

### 2.1 入口清单

| 入口 | 形态与参数 | 注册/拦截 | 关键证据 |
|---|---|---|---|
| `lance_match(col, query [, 'k=v,...'])` | 返回 BOOLEAN 的 V2 标量函数；options 支持 `fuzziness`、`operator=AND|OR`、`boost`、`prefix_length`、`max_expansions` | 目录 `loadFunction` 注册；`LanceFtsPredicateRule` 在 optimizer 把谓词从 Filter 搬进 relation options；`produceResult` 永远抛错 | `L:java/org/lance/spark/function/LanceMatchFunction.java:32-96`、`L:java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java:311-357`、`L:scala/org/apache/spark/sql/catalyst/optimizer/AbstractLanceFtsPredicateRule.scala:65-108,168-273` |
| `lance_match_phrase(col, query [, slop])` | 第 3 参为 Integer 字面量，slop>=0，默认 0；要求索引 `with_position=true` | 同上 | `L:java/org/lance/spark/function/LancePhraseFunction.java:35-100`、`L:.../AbstractLanceFtsPredicateRule.scala:275-295` |
| `lance_multi_match(query [, 'operator=AND'] , col1, col2, ...)` | 第 2 参是含 `=` 的字符串字面量时视为 options；列数 >=2；跨列语义是“任一列命中”，operator 只控制列内 query term 的组合 | 同上；固定 `boosts=null` | `L:java/org/lance/spark/function/LanceMultiMatchFunction.java:48-118`、`L:.../AbstractLanceFtsPredicateRule.scala:298-330` |
| `SEARCH(table=>..., query=>..., search_columns=>..., k/limit/num_results, columns, filter, version, with_row_id)` | Spark 3.5+ 命名参数表函数；`search_columns` 必填；返回所选列 + `_score` | session extension 注入表函数；实现为普通 BatchScan：relation + 可选 Filter + Project + `Sort(_score DESC)` + `Limit(k)` | `L:scala/org/lance/spark/search/LanceSearchTableFunctions.scala:159-213`、`L:lance-spark-3.5_2.12/src/main/scala/org/lance/spark/extensions/LanceSparkSessionExtensions.scala:46-52` |
| `HYBRID_SEARCH(table=>..., query_vector=>..., query=>..., k, ...)` | 向量 + FTS 两路候选，driver 单 task 内做 RRF，输出 `_distance`/`_score`/`_relevance_score` | FTS 侧通过 `LanceSearchQuery(FULL_TEXT)` 走 namespace `queryTable`，向量侧同理；结果在 `LanceHybridSearchRowPartitionReader` 内存合并 | `L:.../LanceSearchTableFunctions.scala:236-321`、`L:java/org/lance/spark/search/LanceHybridSearchRowPartitionReader.java:55-120` |

DataFrame API：没有 `spark.lance.match(...)` 之类的专用入口。可行路径是用 SQL 表达式（`spark.sql(...)`，或 `df.where("lance_match(body, 'x')")`，后者依赖默认 catalog 的函数解析，**推测可用但仓库无测试**），或对 `SEARCH`/`HYBRID_SEARCH` 直接用 SQL（表函数无法从 DataFrame API 调用）。谓词函数的解析依赖“默认 catalog 是 Lance catalog”，文档已经在 `docs/src/operations/dql/fts.md` 的 Prerequisites 里写明这一点。

### 2.2 谓词入口的完整调用链（local 与 namespace 共用前半段）

1. `spark.sql` 解析出 `ApplyFunctionExpression(fn=lance_match, args)`，函数由 `BaseLanceNamespaceSparkCatalog.loadFunction` 返回（`L:java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java:323-357`）。
2. session extension 注册的 `LanceFtsPredicateRule`（`L:lance-spark-3.5_2.12/.../LanceFtsPredicateRule.scala:17-28` 是 5 参 copy 的版本子类）在 optimizer 的 `Filter(condition, LanceRelation)` 上工作：收集谓词、拒绝 OR/多谓词/非顶层 AND/与 nearest 混用，构造 `FullTextQuery`，序列化成 JSON 写进 relation options 的 `fullTextQuery`，非 FTS 残差留在 Filter 里（`L:.../AbstractLanceFtsPredicateRule.scala:65-108`）。
3. `LanceSparkReadOptions` 解析 `fullTextQuery`（`L:java/org/lance/spark/LanceSparkReadOptions.java:67,274,679-681`），`LanceScanBuilder` 构造时校验 schema 不允许存储列 `_score`（`L:.../read/LanceScanBuilder.java:157-160`、`L:java/org/lance/spark/utils/Utils.java:42,53,56-62`）。
4. 路径分叉：`shouldNamespaceFtsScan()` = 非 branch/tag && 有 fullTextQuery && `LanceRuntime.supportsQueryTable(namespaceImpl)` && 没有 pushed aggregation（`L:.../read/LanceScanBuilder.java:600-609`）。
   - 命中 → `buildNamespaceFtsScan()` 组一个单分区 `LanceSearchScan`（§2.3）。
   - 否则 → 普通 `LanceScan` 的 per-fragment 路径（§2.4）。
5. `_score` 列：`LanceDataset` 无条件把 `SCORE_COLUMN` 作为 MetadataColumn 广告（`L:java/org/lance/spark/LanceDataset.java:166-200`）；没有 FTS 谓词却投影 `_score` 时在 `build()` 里报错（`L:.../read/LanceScanBuilder.java:209-218`）。
6. `COUNT(*)`：FTS 谓词不在 `pushedPredicates` 里，所以 metadata count 快捷路径必须单独检查 `getFullTextQuery()`，否则会忽略 FTS；`LanceCountStarPartitionReader` 的 scan 也带 fullTextQuery（`L:.../read/LanceScanBuilder.java:802-804`、`L:.../read/LanceCountStarPartitionReader.java:94-103`）。

### 2.3 namespace 路径

`buildNamespaceFtsScan()`（`L:.../read/LanceScanBuilder.java:616-660`）把所有投影列（`_score` 留在请求列里，`_rowid` 通过 `withRowId`）打包成 `LanceSearchQuery(FULL_TEXT)`：

- `k`：只有 `limit.isPresent() && !hasResidualPredicates` 时才用真实 limit，否则 `Integer.MAX_VALUE`；
- `offset`：同样只在无 residual 时下推；
- `filter`：`pushedPredicates` 编译成 Lance SQL WHERE；
- `prefilter(true)`、pinned `version`（存在时）。

`LanceSearchScan.planInputPartitions()` 恒返回 1 个 partition（`L:.../search/LanceSearchScan.java:39-53`），reader 调 `namespace.queryTable(request).getData()` 拿到整段 Arrow IPC 的 `byte[]`，再用 `ArrowFileReader` 解码（`L:.../search/LanceSearchColumnarPartitionReader.java:80-95`）。`QueryTableRequest` 的字段见 `J:org.lance.namespace.model.QueryTableRequest`（`k`、`offset`、`filter`、`prefilter`、`full_text_query`、`columns`、`with_row_id`、`version` 等），响应只有 `data` + `context`（`J:...QueryTableResponse`）。

`SEARCH()` 走的是同一条路：`search()` 把 fullTextQuery 写进 relation options，再叠 Filter/Project/Sort/Limit；Spark 的 TopN 下推只在 `orders.length==1 && _score DESC && !hasResidualPredicates && shouldNamespaceFtsScan()` 时接受（`L:.../read/LanceScanBuilder.java:734-748`，测试 `T:.../read/BaseFtsScoreColumnTest.java:166-237` 直接断言了请求里的 k=2 与 k=Integer.MAX_VALUE）。

### 2.4 local 路径

- 切分：`LanceSplit.planScan` 每个 fragment 生成一个 split，没有合并（`L:.../read/LanceSplit.java:92-103`）；`LanceScan.planInputPartitions` 一个 split 一个 `LanceInputPartition`（`L:.../read/LanceScan.java:453-507`）。
- 每 task：`LanceFragmentScanner.create` 打开 dataset，`dataset.getFragment(id).newScan(scanOptions)`；FTS 时设置 `fullTextQuery`、`prefilter(true)`、`disableScoringAutoprojection(true)`，并在投影里显式加 `_score`；limit/offset 来自 partition；`collectStats(true)`（`L:.../internal/LanceFragmentScanner.java:77-135`，limit/offset `:125-131`，`_score` `:92-96`，stats `:132`）。
- Spark 侧：因为没有下推 ranked top-k，`WHERE lance_match(...)` 无 LIMIT 时每个 fragment 返回全部命中，Spark 做全局 Sort/Filter/Limit；只有普通 LIMIT 会被 `pushLimit` 接收（`L:.../read/LanceScanBuilder.java:701-708`），并且 `pruneByLimit` 对 FTS 显式禁用，避免按 fragment 行数裁掉未调度的命中（`L:.../read/LanceScan.java:623-631`，测试 `T:.../read/BaseFtsMatchPushdownTest.java:192-230`）。

## 3. Lance 14 的 FTS 真实语义（问题 2）

以下每条都区分“能确认”和“只能推测/需实测”。

### 3.1 打分

能确认（Rust 源码）：

- BM25 参数固定 `K1=1.2`、`B=0.75`（`R:rust/lance-index/src/scalar/inverted/scorer.rs:88-105`）。
- `idf(t) = ln((N - df + 0.5) / (df + 0.5) + 1)`，`N` 是索引覆盖的文档数（`R:.../scorer.rs:236-247`）。
- `doc_weight = (K1+1)*tf / (tf + K1*(1 - B + B*dl/avgdl))`，分数是各 query token 的 `idf * doc_weight` 之和；`dl` 是该文档 token 数，`avgdl` 是索引平均文档长度（`R:.../scorer.rs:158-200,264-330`）。
- 多列 `multi_match` 是“每列独立 BM25 取最大分”，不是 BM25F；`combined_fields` 才是 BM25F（跨列混合 df/avgdl），且 Lance 14 已经实现（`R:rust/lance/src/dataset/scanner.rs:5687-5755`、`R:.../scorer.rs:302-360`）。连接器没有暴露 `combined_fields`（`J:org.lance.ipc.FullTextQuery` 有 `combinedFields` 工厂，但 `FullTextQueryUtils` 只处理 MATCH/MATCH_PHRASE/MULTI_MATCH）。

能确认（Java/行为）：

- `_score` 是 nullable `Float32`（`L:java/org/lance/spark/LanceDataset.java:166-188`、`T:.../read/BaseFtsScoreColumnTest.java:481-490`），来自 native scan 的 `_score` 列；连接器设置 `disableScoringAutoprojection(true)` 并自己把 `_score` 放进投影，避免 native 自动追加造成重复列（`L:.../internal/LanceFragmentScanner.java:92-96,116-118`；`R:.../scanner.rs:2919-2960` 的 auto-projection 语义）。

### 3.2 ranked / unranked、limit 与 offset

能确认：

- Java `FullTextQuery` 没有“unranked/只过滤”的模式；Match/Phrase/MultiMatch 始终带打分。所谓 ranked，是“有没有 limit 时是否只回 top-k”。
- native scanner 的 `fts()` 在 query 未自带 limit 时设置 search limit：`limit` 与 `offset` 都有 → `limit+offset`；只有 `limit` → `limit`；只有 `offset` → 留空，由上层 `GlobalLimitExec` 处理（`R:.../scanner.rs:5326-5338`）。索引搜索把 `params.limit` 当 top-k 上界，`None` 等价 `usize::MAX`，即返回全部命中（`R:rust/lance-index/src/scalar/inverted/index/search.rs:761-766`），并用大小为 limit 的堆归并（`R:.../search.rs:883-909`）。
- 顶层 `Limit/Offset` 由 `GlobalLimitExec` 在 ranked source 之后执行（`R:.../scanner.rs:3915-3918,8547-8554`）；因此 `offset` 是“排序后的跳过行数”，`limit` 是排序后的截断行数。
- `HybridCompoundQueryExec.with_indexed_input` 的输入契约明确要求“至多 limit 行、按 score 降序、rowid 升序”（`R:rust/lance/src/io/exec/fts.rs:1077-1083`），这是 native 内部可组合的 bounded top-k 约定；单列 `MatchQueryExec` 的实际输出只保证按 score 返回，没有额外的 rowid tie-break（`R:.../fts.rs:3029-3240`，合并用 `ScoredDoc` 只比 score）。
- 索引分支 + 未索引/过期行的混合分支才会显式补 `SortExec(score DESC)`；纯索引单列分支本身不在 `combine_fts_leaf_plans` 里加全局 sort（`R:.../scanner.rs:6163-6195`）。最终用户看到的顺序由 Spark 的 `Sort` 决定。

只能推测/需实测：

- `queryTable`（namespace 服务端）对 `k`/`offset` 是否严格按“全局 score 排序后跳过 offset、取 k”执行；k 是否有服务端上限。连接器只在“无 residual 的 `_score DESC LIMIT k`”上依赖它，测试断言了返回集合与全排序一致，但没有覆盖 tie、offset>0、k 很大三种情况（`T:.../read/BaseFtsScoreColumnTest.java:166-237`）。

### 3.3 多列 fts_columns

能确认：

- `lance_multi_match` → `FullTextQuery.multiMatch(queryText, columns, null, operator)`（`L:.../AbstractLanceFtsPredicateRule.scala:322`）；native 侧对每列独立打分，按 row 取最大分（`R:.../scanner.rs:5687-5755`），所以“任一列命中即行命中”是语义的一部分；列内 term 组合由 `operator` 控制。
- 连接器没有暴露每列 boost（`buildMultiMatchQuery` 固定传 `null`），也没有 `combined_fields`；namespace 转换时 `MultiMatchQuery` 会被展开成每列一个 `MatchQuery`（`L:java/org/lance/spark/utils/FullTextQueryConverter.java:97-107`），这条路径与 native 的“max per row”是否完全等分需实测。
- `SEARCH(search_columns => array(...))` 在多列时也用 `multiMatch`（`L:.../LanceSearchTableFunctions.scala:184-187`）。

### 3.4 短语、模糊、tokenizer/analyzer/language

能确认：

- 短语要求 `with_position=true`（`docs/src/operations/dql/create-index.md:87-102`、`docs/src/operations/dql/fts.md:151-159`）；`slop` 是词间最大距离，默认 0（`L:.../AbstractLanceFtsPredicateRule.scala:275-295`）。native 侧 `phrase_slop` 参数与位置索引配合（`R:rust/lance-index/src/scalar/inverted/query.rs:70-74`）。
- 模糊：`fuzziness` 省略 = 自动编辑距离，`Some(0)` = 精确；自动距离由 `MatchQuery::auto_fuzziness(token)` 按 token 长度决定（`R:.../query.rs:12-20,43`），`prefix_length` 按 Unicode 字符计（`R:.../query.rs:29-53`），`max_expansions` 是整个 query 的全局预算（`R:.../query.rs:66-69`）。文档的 “When omitted, the engine determines fuzziness by token length” 与代码一致；测试用 `hell` 证明 `fuzziness=1` 严格包含 `fuzziness=0`（`T:.../read/BaseFtsAdvancedOptionsTest.java:238-283`）。
- query 侧分词用的是“该索引的 tokenizer”，不是 query 自带的 analyzer（`R:rust/lance/src/io/exec/fts.rs:3188-3192`），所以索引建错 tokenizer 时查询无法从 SQL 侧补救。
- 索引侧可配置项远多于连接器文档列出的 8 个：`analyzer`（text/code）、`lance_tokenizer`（text/json）、`base_tokenizer`（simple/whitespace/raw/ngram/code/icu/lindera/jieba）、`language`、`with_position`、`max_token_length`、`lower_case`、`stem`、`remove_stop_words`、`custom_stop_words`、`ascii_folding`、`min/max_ngram_length`、`prefix_only`、`block_size`、`split_identifiers`、`split_on_numerics`、`preserve_original`、`index_operators`、`format_version`、`document_granularity`（`J:org.lance.index.scalar.InvertedIndexParams$Builder`）。连接器 CREATE INDEX 文档只列了 simple/ngram 与 8 个布尔/字符串参数（`docs/src/operations/dql/create-index.md:87-102`），这是文档缺口，不是 native 能力缺口。

### 3.5 返回列、文档粒度、未索引行

能确认：

- FTS scan 总会额外产生 `_score`（Float32, nullable）；LIST_ELEMENT 粒度还会产生 `_doc_index`（`R:.../scanner.rs:2862-2905`）。连接器只投影 `_score`，不暴露 `_doc_index`，也没有 `document_granularity` 选项，默认 ROW（`J:org.lance.DocumentGranularity`）。
- 未索引 fragment / 索引建立后新增的行：native 会走 flat text 分支，用已提交索引的语料统计 + 当前数据重新打分（`R:.../scanner.rs:4628-4668,6210-6250`）。仓库文档明确提示 partial index 的 score/rank 可能在 `OPTIMIZE INDEX` 前后变化（`docs/src/operations/dql/fts.md:145-150`，引用 Lance issue #9058）。
- `fast_search=true` 会丢弃未索引行（只走索引），连接器的 FTS 谓词路径不设置该参数（`L:.../internal/LanceFragmentScanner.java:113-118` 只设 fullTextQuery/prefilter/disableScoringAutoprojection），默认 `false`。

只能推测/需实测：

- 未索引行走 flat 分支时 `_score` 是否与已索引行走同一把尺子（代码上用的是同一 committed scorer + 当前文本 token，但仓库文档承认会有差异）；需要固定 snapshot 下逐行对比。

## 4. 分布式执行与 rank 合并（问题 3）

### 4.1 local：每 fragment 一 task，rank 在 Spark 侧合并

- 切分粒度是一个 fragment 一个 Spark partition（`L:.../read/LanceSplit.java:92-103`），没有 grouped fragments。
- 每个 task 的 native scan 只把该 fragment 放进 `fragmentIds`（`J:org.lance.Fragment.newScan` 会用单 fragment 覆盖 options.fragmentIds，`J:.../Fragment.java:99-106`）；但 FTS 的索引搜索仍会打开该列的**全部 committed segments**，并用 `fragments_covered_by_fts_query` 与目标 fragment 求交来做 mask（`R:.../scanner.rs:5005-5018`），BM25 统计来自打开的全部分区（`R:.../scorer.rs:264-300`）。这解释了仓库文档的成本模型 `N_fragments × M_segments` 次 segment open（`docs/src/operations/dql/fts.md:161-165`）。
- limit：只有 Spark 的普通 LIMIT 会通过 `pushLimit` 进入每个 fragment（`L:.../read/LanceScanBuilder.java:701-708`），native 据此做该 fragment 的 top-k；`ORDER BY _score DESC LIMIT k` 不会走 `pushTopN`（条件要求 `shouldNamespaceFtsScan()`，`L:...:734-748`），所以每个 fragment 返回全部命中，Spark 全局排序后再 limit。
- `pruneByLimit` 对 FTS 禁用，避免按 fragment 行数提前停止调度（`L:.../read/LanceScan.java:623-631`；测试 `T:.../read/BaseFtsMatchPushdownTest.java:209-230` 证明 fragment-2 必须被调度）。

### 4.2 namespace：单 partition，服务端全局 top-k

- `LanceSearchScan` 恒 1 个 partition（`L:.../search/LanceSearchScan.java:51-53`），k 与 offset 的执行都在 `queryTable` 服务端；连接器不做二次归并。
- 无 residual 时 k=limit，与 Spark 的 `Sort(_score DESC)`+`Limit` 对齐；有 residual 时 k=`Integer.MAX_VALUE`，Spark 再过滤/截断（`L:.../read/LanceScanBuilder.java:632-634`）。测试 `T:.../read/BaseFtsScoreColumnTest.java:167-183` 断言了这两种 k。

### 4.3 有没有 recall 缺口？

按当前代码，**没有“拿分片 top-k 冒充全局 top-k”的缺口**：

- local 路径不做全局 top-k 下推，Spark 保留全局 Sort/Limit（`L:.../read/LanceScanBuilder.java:734-748` 的 `!shouldNamespaceFtsScan()` 分支；测试 `T:.../read/BaseFtsScoreColumnTest.java:186-207` 证明 pushTopN 拒绝后查询结果仍正确）。
- namespace 路径是单 partition，服务端自己算全局 top-k，Spark 不再归并。
- 普通 `LIMIT k`（无 ORDER BY）在 local 路径会把 k 下推到每个 fragment，每个 fragment 返回自己的 top-k，Spark 的 `CollectLimit` 取前 k 行；这对“未排序 LIMIT”是合法语义（结果是任意 k 行），但不是“全局最好的 k 行”。如果用户把 `LIMIT` 当 top-k 用，需要显式 `ORDER BY _score DESC`。

但有三个真实缺口/隐患：

1. **tie 不稳定**：`ScoredDoc` 的 `Ord` 只比较 score（`R:.../builder.rs:2006-2010`），WAND 的堆、per-segment 合并、`combine_fts_leaf_plans` 的 `SortExec` 都只按 score（`R:.../fts.rs:305-370`、`R:.../scanner.rs:6163-6195`）；只有 multi_match/compound 的 `fts_score_sort_exprs` 显式加了 `rowid ASC`（`R:.../scanner.rs:6270-6298`）。这意味着**同一查询重复执行、或按 offset 翻页时，同分行的顺序不保证稳定**；如果 #3263 用 offset continuation，必须有 (score, rowid) 全序。
2. **partial index 下 score 不可比**：仓库文档已承认（`docs/src/operations/dql/fts.md:145-150`）；跨 fragment 合并排名时，未索引行与已索引行的 score 可能不同源，需要固定 snapshot 的专项验证。
3. **大 k / 大结果集内存**：namespace 路径把整个响应当 `byte[]` 在单 task 解码（`L:.../search/LanceSearchColumnarPartitionReader.java:80-95`），residual 时 k=MAX_VALUE；HYBRID_SEARCH 还会把两侧结果全量读进 `List<SideRow>` 再 RRF（`L:.../search/LanceHybridSearchRowPartitionReader.java:55-90`）。10M 行、常见词、宽列时这是首要 OOM/IPC 风险，而不是 CPU。

### 4.4 `_score` 排序下推的确切条件

`pushTopN` 只在同时满足以下条件时接受：`topN_push_down` 开启、无 residual、`orders.length==1`、order 是 `FieldReference` 且字段名 `_score`、降序、`shouldNamespaceFtsScan()`（`L:.../read/LanceScanBuilder.java:734-748`）。任何多键排序（`_score DESC, id ASC`）、升序、residual 都会退回 Spark 全局排序。

## 5. 观测现状与建议（问题 4）

### 5.1 现在能报什么

| 层 | 能报 | 不能报 | 证据 |
|---|---|---|---|
| local Spark Scan | 12 个 custom metrics：`numFragmentsScanned`、`numBatchesLoaded`、`numRowsScanned`、`numIops`、`numRequests`、`numBytesRead`、`numIndicesLoaded`、`numPartsLoaded`、`numIndexComparisons`、`datasetOpenTimeNs`、`scannerCreateTimeNs`、`batchLoadTimeNs` | `ScanStats` 里的 `indexCacheHits/Misses` 和 `allCounts/allTimes` 没有映射；没有 FTS 专属计数（命中候选数、posting/segment 打开数、打分耗时） | `L:.../read/metric/LanceCustomMetrics.java:40-48,198-210`、`L:.../read/metric/LanceReadMetricsTracker.java:206-216`、`J:org.lance.ipc.ScanStats` |
| local plan | `LanceScan.getMetaData()` 暴露 `whereConditions/limit/offset/topNSortOrders/pushedAggregation/originalFragmentCount/numPartitions/fullTextQuery` | 没有每 fragment 的候选/命中量；没有 native stage 时间 | `L:.../read/LanceScan.java:1081-1121` |
| namespace Scan | 只有 Spark 的 output rows/bytes | 无 custom metrics、无 `getMetaData`、无 statistics；`description()` 只有类名；`QueryTableResponse.context` 被丢弃；没有 connect/request/IPC/decode 的计时。`LanceSearchColumnarPartitionReader` 没有覆盖 `currentMetricsValues()`，`LanceSearchRowPartitionReader` 只是把 `reader.currentMetricsValues()` 委托过去，得到 Spark 的默认空数组（Spark 3.5.5 `PartitionReader` 有 default 方法） | `L:.../search/LanceSearchScan.java:24-53`、`L:.../search/LanceSearchColumnarPartitionReader.java:80-95`、`L:.../search/LanceSearchRowPartitionReader.java:64-70`、`J:...QueryTableResponse` |
| namespace 服务 | `Dataset.getIndexStatistics(String)` 可给出索引覆盖（indexed/unindexed rows/fragments、num_segments、每段统计） | 没有“本次查询”的 plan/metrics；统计是静态索引信息 | `J:org.lance.Dataset#getIndexStatistics`、`R:rust/lance/src/index.rs:3303-3320,3607-3651` |

### 5.2 建议补的指标（名字/口径/单位/层）

| 指标 | 层 | 口径 | 单位 |
|---|---|---|---|
| `numNamespaceRequests` | namespace Spark Scan | 本 task 实际发出的 `queryTable` 次数（分页后 >1） | 次 |
| `namespaceConnectTimeNs` / `namespaceRequestTimeNs` | namespace Spark Scan | `getOrCreateNamespace` 与 `queryTable` 调用的墙钟；分开记录，不合并成“搜索耗时” | ns（用 `CustomNsTimeMetric`） |
| `numIpcBytesReceived` | namespace Spark Scan | `QueryTableResponse.data.length` 之和，不含 Arrow 解码后的堆 | bytes |
| `namespaceDecodeTimeNs` | namespace Spark Scan | `ArrowFileReader` 初始化 + `loadNextBatch` 的累计时间 | ns |
| `numPages` / `numRowsReturned` | namespace Spark Scan | 分页数、返回给 Spark 的行数（过滤前） | 次 / 行 |
| `numIndexCacheHits` / `numIndexCacheMisses` | local Spark Scan | 直接转发 `ScanStats.getIndexCacheHits/Misses`（当前被丢弃） | 次 |
| `numFtsMatchedRows` | local/namespace | native 侧返回的命中行数（local 可在 reader 累计 `numRowsScanned` 之外单独记；namespace 可直接记返回行数） | 行 |
| `ftsSearchTimeNs`、`ftsLoadPostingTimeNs`、`ftsScoreTimeNs` | native（core） | 复用 `MetricsCollector`/`IndexTiming` 的粗粒度 stage 计时，经 `ScanStats.allTimes` 透出 | ns |

`ScanStats.allCounts/allTimes` 目前是原样 map（`J:org.lance.ipc.ScanStats#getAllCounts/getAllTimes`），键名由 native 决定；连接器应先做一个只读 probe（本地小数据集打印一次），确认键集合后再决定映射哪些，不要把未验证的键写死。

## 6. 测试与文档覆盖（问题 5）

### 6.1 测试覆盖

| 测试类（base + 版本子类） | 后端 | 已覆盖 | 缺口 |
|---|---|---|---|
| `BaseFtsMatchPushdownTest` / `FtsMatchPushdownTest` | local + namespace（catalog-only 子类） | 基本命中、多 token、跨 fragment、LIMIT 不裁剪 fragment、FTS+scalar、OR/重复/非顶层/运行时参数/nearest 的错误 | 没有 tie、offset、大 k、分页 |
| `BaseFtsPhraseMatchPushdownTest` | local + namespace | 精确短语、slop=1 超集、错误边界、residual 保留 | 没有跨 fragment 的位置边界、没有 namespace offset |
| `BaseFtsAdvancedOptionsTest` | local + namespace | operator/boost/prefix/max_expansions/fuzziness、multi_match OR/AND/大小写、未知 key/非法值 | 没有打分一致性（OR 超集不等于分数正确）、没有 multi_match 的 max 语义断言、没有 `combined_fields` |
| `BaseFtsScoreColumnTest` | local + namespace | `_score` 可空、只对 FTS 可见、stored `_score` 拒绝、排序、limit、namespace TopN（k=2 vs MAX_VALUE）、`_score` residual 保持 residual | 没有 tie 稳定性、没有 offset/翻页、没有 partial index 的分数漂移 |
| `BaseFtsCatalogOnlyNamespaceTest` | catalog-only | local fallback、COUNT(*) 尊重 FTS | 没有覆盖“namespace override 但 structured 能力不全”的实现 |
| `FullTextQueryUtilsTest` / `FullTextQueryConverterTest` | 单测 | 序列化/反序列化/结构化相等/namespace 转换 | 没有 multi_match 打分等价、没有 BOOST/BOOLEAN 的显式拒绝路径覆盖（有 converter 单测） |
| `BaseSparkSearchTableFunctionTest` | local + namespace | `SEARCH`、`HYBRID_SEARCH`、命名参数、stored score 拒绝 | 没有 FTS 的 offset、没有大 k、没有 metrics |

Python 集成测试只有 `test_search_table_function` 和 `test_hybrid_search_table_function` 两个 FTS 相关用例（`integration-tests/test_lance_spark.py:1960-2035`），建索引参数固定为 simple/English/with_position，没有覆盖 tokenizer 变体、partial index、residual、分页。

### 6.2 文档覆盖

`docs/src/operations/dql/fts.md` 已经写清入口、参数、`_score`、ranked 路径差异、已知限制和成本模型，是当前最完整的资料。缺口：

- 没有 BM25 公式、k1/b、`N/df/avgdl` 的全局性；用户无法判断“为什么同名 query 在不同 fragment 配置下分数不同”。
- 没有 `multi_match` 的“每列独立 BM25、按行取最大”语义（只写了 OR/AND）。
- 没有 `combined_fields`（BM25F）、`document_granularity`/`_doc_index`、`boosts`（SQL 未暴露）。
- tokenizer 只列 simple/ngram，native 支持的 analyzer/code/icu/lindera/jieba/whitespace/raw 未列（`docs/src/operations/ddl/create-index.md:87-102`）。
- 没有 namespace 与 local 两条路径的 plan/指标/耗时口径说明；没有 offset/翻页语义。

## 7. 未知/待实测（必须与“已确认”分开）

| # | 未知项 | 为什么重要 | 最小验证 |
|---|---|---|---|
| U1 | namespace `queryTable` 的 FTS 结果是否有稳定全序（同分行按 rowid？）、`offset` 是否在全局排序后精确跳过、k 是否有服务端上限 | 直接决定 #3263 能否用 offset continuation | DirectoryNamespace + 小数据集，构造大量同分行；k=1..N 逐页取，比较并集/多重集与全量结果；重复 3 次比较顺序；再测 offset>0、k 很大 |
| U2 | 单列 `lance_match` 在 `limit` 下的 top-k 是否精确（WAND exactness、tie、fuzzy/phrase） | 决定“每分片 top-k 合并”是否有 recall 缺口 | 本地小数据集 + 暴力全量打分对照；同分行、fuzzy、phrase、slop 各一组；k 从 1 到 N |
| U3 | Spark 在 `pushTopN=false` 时是否总保留全局 Sort/Limit，且 `pushLimit`/`pushTopN` 的调用顺序与 `hasResidualPredicates` 的时序稳定 | local 候选 hint 方案的前提 | 用 `EXPLAIN`/executedPlan 断言 `Sort`+`CollectLimit` 仍在；覆盖 LIMIT、ORDER BY、ORDER BY+LIMIT、residual 四种 |
| U4 | `_score` 在 namespace 与 local 是否同值同序（同 snapshot、同索引）；partial index 下未索引行的分数是否可比 | 跨路径正确性与“能否统一排序键” | 同一份本地数据集分别走两条路径，逐行比较 id/score；再 append 未索引行后重复 |
| U5 | `queryTable` 的 `filter` 与 `prefilter=true` 是否严格“先过滤再排序”；`_rowid` 是否可作为 filter 列 | 决定 residual 下推与 rowid 分块是否可行 | 构造“过滤后只剩少量命中”的数据集，比较 filter+prefilter 与 Spark 侧过滤后的结果；用 `_rowid >= x` 测过滤 |
| U6 | `SEARCH`/`HYBRID_SEARCH` 的 FTS 侧在 catalog-only/无 queryTable 时的行为 | 决定 fallback 覆盖面 | 现有 catalog-only 测试只覆盖谓词路径，加表函数用例 |
| U7 | `ScanStats.allCounts/allTimes` 的键集合、FTS 是否贡献 `indexCacheHits/Misses` | 决定观测映射 | 本地跑一次 FTS scan，打印 `getStats()`；同一查询重复跑观察 hits/misses 变化 |
| U8 | `wand_factor`（默认 1.0）与 exactness certificate 在什么条件下会返回近似 top-k | 决定“有界 top-k 是否可作为精确结果” | 本地对比不同 `wand_factor`（需要 native 暴露；连接器未暴露，先读 core 测试） |

## 8. #3263 方案（ranked residual 的有界执行）

### 8.1 目标与不变量

先固定语义，再谈性能：

- I1 `WHERE lance_match(...)` 无 LIMIT：必须返回全部命中（不能拿 top-k 替代）。
- I2 `WHERE lance_match(...) AND <residual>` 无 LIMIT：返回 residual 过滤后的全部命中。
- I3 `ORDER BY _score DESC LIMIT k`（无 residual）：全局 top-k，k 可下推（现状已如此）。
- I4 `ORDER BY _score DESC LIMIT k` + residual：必须返回“过滤后按 score 排序的前 k 行”，不能截漏。
- I5 多键排序（`_score DESC, id ASC`）、升序、`_score` 过滤：行为不变，由 Spark 全局排序/过滤。
- I6 固定 snapshot：一次查询内所有分页/分片看到同一 version；取消/重试有界。
- I7 返回列契约不变：`_score` 仍只在显式投影时出现，`SELECT *` 不变。

### 8.2 改动 A：严格等价过滤下推（先做，收益最大且风险最低）

现状：`FilterPushDown.isPredicateSupported` 只认 And/Or/Not、比较、IS [NOT] NULL、IN（`L:.../read/FilterPushDown.java:90-121`），且 `_score`、blob v1 虚拟列被强制 residual（`L:.../read/LanceScanBuilder.java:157-166`）。Spark 里常见的 `MOD(id,2)=1`、`LIKE`、`startsWith`、算术表达式都会变成 residual，触发 namespace 的 `k=MAX_VALUE`。

改动：

1. 在 `FilterPushDown` 增加“可序列化为 Lance SQL 的表达式白名单”，把 Spark 表达式翻译成 DataFusion 能等价求值的 SQL 文本：布尔组合、比较、`IS [NOT] NULL`、`IN`、算术（`+ - * / %`）、`LIKE`（含转义）、`startsWith/endsWith/contains`（改写成 `LIKE` 并转义 `%/_`）、确定性的 `lower/upper/length`（先用差分测试证明 Unicode/空值语义一致再放开）。不在白名单的继续 residual。
2. 增加差分测试 `FilterPushdownEquivalenceTest`：同一份含 null、负数、Unicode、Decimal、空字符串的 fixture，分别用 Spark residual 与下推过滤求结果，要求逐行相等；新增映射必须有对应用例。
3. `_score` 仍然 residual（不能作为 prefilter 条件，否则语义变成“先按未知分数过滤再排名”），`_rowid` 是否可下推取决于 U5。

预期：residual 比例显著下降，`k=MAX_VALUE` 的触发面缩小；这一步单独就能作为 #3263 的第一批提交，且不改变任何返回契约。

### 8.3 改动 B：namespace 有界读取（核心）

目标：把 `k=Integer.MAX_VALUE` 改成“固定 snapshot + 稳定全序 + continuation 分批”，但**只有在 U1 证明服务端全序稳定时才启用**。

实现（建议新增类而不是改 `LanceSearchScan` 的单分区契约）：

- 新类 `LanceSearchPagedColumnarPartitionReader`（同包 `org.lance.spark.search`，复用 `LanceSearchColumnarPartitionReader` 的 Arrow 解码逻辑）。注意 `LanceSearchPartitionReaderFactory` 的 row 模式只是把 `LanceSearchColumnarPartitionReader` 包一层（`L:.../search/LanceSearchPartitionReaderFactory.java:29-42`、`L:.../search/LanceSearchRowPartitionReader.java:24-40`），所以分页只改一处即可同时覆盖 row/columnar 两种读取：
  - 构造：`(LanceSearchInputPartition partition, int pageSize, long baseOffset)`；
  - 状态：`consumedRows`、`exhausted`、当前页 `ArrowFileReader`；
  - `next()`：当前页读完且未耗尽时，发下一页请求：`query.toQueryTableRequest(baseOffset + consumedRows, pageSize)`，读完一页后 `consumedRows += pageRows`；当某页返回行数 < pageSize 时标记 `exhausted`；
  - `close()`：关闭当前页并禁止再发请求（取消/重试安全）；
  - 计数：requests/rows/bytes/decode 时间（§5.2）。
- `LanceSearchQuery` 增加重载 `toQueryTableRequest(int requestOffset, int requestK)`；保留现有方法委托到 `(offset, k)`，`J:org.lance.namespace.model.QueryTableRequest` 已有 `offset`/`k` 字段。
- `LanceScanBuilder.buildNamespaceFtsScan()`：当 `pagingEnabled && (hasResidualPredicates || !limit.isPresent())` 时，用 `pageSize` 构造 `LanceSearchScan`，并把 base offset（若 Spark 下推了 offset）带进 reader；当无 residual 且 limit 已下推时保持单页（现状）。
- 配置（新增，默认保守）：
  - `spark.sql.lance.fts.namespace.paging.enabled`（默认 `false`，先离线验证再翻默认）；
  - `spark.sql.lance.fts.namespace.page.size`（默认 4096，范围 1..65536）；
  - `spark.sql.lance.fts.namespace.max.pages`（默认 10000，防服务端不返回短页导致死循环）。
- 失败模式：分页请求异常 → `IOException`，任务重试从头开始（version 已 pin，结果一致）；超过 `max.pages` → 明确报错并给出“缩小 filter / 改用 local 路径 / 关闭分页”的提示。

正确性论证：若服务端对同一 snapshot、同一 query 返回的排序是稳定的全序 `(score DESC, rowid ASC)`，则 `offset=0,pageSize`、`offset=pageSize,pageSize` … 的分页并集等于全量结果，无重复无遗漏；residual 由 Spark 在上层过滤，不参与分页位置，因此不会截漏。I1/I2 在分页耗尽时自然成立。

**前提 U1 不成立时不要启用 B**；改用 B'（§8.4）或保持现状并只加观测。

### 8.4 改动 B'：rowid 范围递归切分（U1 不成立时的精确后备）

如果服务端排序不稳定但 `_rowid` 可作为 filter（U5），用“按 rowid 值域递归二分”替代 offset 分页：

- 对区间 `[lo, hi)` 发请求：`filter = baseFilter AND _rowid >= lo AND _rowid < hi`，`k = pageSize`；
- 返回行数 < pageSize ⇒ 该区间已穷尽，输出；
- 返回行数 == pageSize ⇒ 区间可能还有，二分 `[lo, mid)`、`[mid, hi)` 递归；
- 初始区间从数据集的 rowid 值域导出（需要 driver 侧读一次 `_rowid` 的 min/max 或 fragment/rowid 统计；实现前先确认 Lance 的 rowid 是否稳定、是否可从 metadata 便宜获得）。

优点：不依赖任何排序；每个请求都有界。缺点：请求数可能多（与命中集大小/分页大小相关），且要求 `_rowid` 过滤下推；如果 rowid 是随机 64 位（stable row ids），区间划分的负载可能不均。结论：作为 B 的 fallback，不默认启用。

### 8.5 改动 C：local 候选上限（可选，默认关）

local 路径每个 task 的内存本来就以 fragment 为界，问题主要是“无 residual 但带 `ORDER BY _score DESC LIMIT k`”时每 fragment 返回全部命中、Spark 全局排序。可以在“无 residual、单键 `_score DESC`、全局 Sort+Limit”时给每个 fragment 一个候选上限 `k+offset`（每 fragment 取自己的前 `k+offset` 行，Spark 仍做全局 Sort+Limit）：全局 top-k 必然落在各 fragment 前 `k+offset` 的并集里，语义正确（同分行的取舍本来就是任意）。

实现方式（不依赖 `pushTopN`，避免 Spark 误以为已全局下推）：在 optimizer 里新增一个规则（或扩展现有 FTS 规则）识别 `Limit(k, Sort([_score DESC], global, child))` 且 child 的 Lance relation 带 fullTextQuery、无 residual、无 offset 之外的复杂结构，把 `candidateLimit=k` 写进 relation options；`LanceScanBuilder` 读到后只作为 local 的 `limit` 传给 `LanceScan`/`LanceInputPartition`，**不改变 `pushTopN` 的返回值**（Spark 继续保留 Sort+Limit）。配置 `spark.sql.lance.fts.local.candidate.limit.enabled` 默认 `false`，先做 plan/正确性测试再打开。

风险：规则必须证明“Filter 已被完全下推、residual 为空”，否则会截漏（I4 的反例）；多键排序、升序、`_score` 过滤都必须跳过。若实现不能保证，宁可不做（留给 #3264 的 grouped fragment + 全局 scorer）。

### 8.6 改动 D：确定性与观测

- 确定性：短期在测试里要求 `ORDER BY _score DESC, _rowid ASC` 作为可复现排序；中期若要支持用户级翻页，需要 native 在单列 match 的堆/合并处也用 `(score, rowid)` 全序（core 改动，属 #3264/后续，不在 #3263 硬做）。
- 观测：按 §5.2 给 namespace reader 加 custom metrics；`LanceSearchScan` 实现 `SupportsMetadata`（与 `LanceScan` 同样的 Spark 接口，`L:.../read/LanceScan.java:56,86,1081`）暴露 `k/offset/pageSize/prefilter/columns/queryType/version`；`description()` 带上这些信息；local 路径补 `indexCacheHits/Misses` 映射（`ScanStats` 已有字段，tracker 只是没取）。
- 指标不要混口径：namespace 的 `requestTime` 包含网络+服务端，不能和 local 的 `batchLoadTime` 直接比；报告中分开列。

### 8.7 与 lance-core 14 的兼容性

- 改动 A/B/B'/C/D 都不需要改 `lance-core` Java API：`ScanOptions` 已有 `fullTextQuery/prefilter/limit/offset/collectStats/disableScoringAutoprojection`（`J:org.lance.ipc.ScanOptions`），`QueryTableRequest` 已有 `k/offset/filter/prefilter/version`（`J:...QueryTableRequest`）。
- 只有“native 全序 (score,rowid)”或“共享全局统计注入”需要 core 扩展：Java 侧 `ScanOptions` 没有 stats/scorer 字段，`FullTextQuery` 也没有（`J` 检查）；`Dataset.getIndexStatistics` 只能读静态覆盖信息（`J`/`R:.../index.rs:3303-3320,3607-3651`），不能注入。
- 兼容性风险：`ScanOptions` 的构造器有多个重载（`J` 里 3 组），新增字段要走 Builder；我们只读不扩展，保持 14.0.3 二进制兼容。

## 9. 验证计划

### 9.1 本地离线（不占集群窗口，秒级~十几秒/类）

新增/扩展测试（base 模块写一次，版本模块用空子类继承，沿用现有 `FtsScoreColumnTest extends BaseFtsScoreColumnTest` 模式）：

1. `BaseFtsPagingTest`（namespace，DirectoryNamespace）：
   - 同分行数据集（所有文档 body 相同，score 全相等），pageSize=1/2/3/7 与单次全量对比：并集逐行相等、无重复、行数相等；
   - 重复 3 次，比较顺序（验证 U1）；
   - residual：`_score > 0`、`MOD(id,2)=1` 各一组，`ORDER BY _score DESC LIMIT k` 与全量基准对比；
   - 无 LIMIT 的 `WHERE lance_match(...)`：分页耗尽后行数等于全量；
   - `OFFSET` + 分页组合（Spark 下推 offset 的单 fragment 场景）；
   - 并发 append 后重跑：pinned version 下结果不变（用 `version` 参数固定）。
2. `BaseFtsTieBreakTest`（local + namespace）：`ORDER BY _score DESC, _rowid ASC` 与 `ORDER BY _score DESC` 的分数序列一致；重复执行顺序稳定（若不稳定，记录为已知限制而不是强断言）。
3. `BaseFtsPushdownEquivalenceTest`：对 A 中新增的每个映射（MOD、LIKE、startsWith、算术、lower/upper）做 Spark residual vs 下推的逐行对比，含 null/Unicode/负数/Decimal。
4. `BaseFtsCandidateLimitTest`（C，若实现）：`EXPLAIN` 断言 `Sort`+`CollectLimit` 仍在；`getMetaData` 有 `candidateLimit`；结果与全量基准的 id 集合在“同分行可任意取舍”的约定下一致（比较分数多重集和 top-k 分数分布）。
5. `BaseFtsMetricsTest`：namespace reader 的 `numNamespaceRequests/numIpcBytesReceived/numRowsReturned` 在分页时 >0 且与页数一致；local `indexCacheHits/Misses` 至少在 `getMetaData`/metrics 里可见。
6. 回归：`FtsMatchPushdownTest`、`FtsPhraseMatchPushdownTest`、`FtsAdvancedOptionsTest`、`FtsScoreColumnTest`、`FtsCatalogOnlyNamespaceTest`、`BaseSparkSearchTableFunctionTest` 全绿。

命令（单条 <10 分钟）：

```bash
cd /Users/jyxc-dz-0101035/workspace/lance-spark-wt/3262
./mvnw -s /tmp/dsh-mvn-settings.xml -q -pl lance-spark-3.5_2.12 -am \
  -Dtest='FtsPagingTest,FtsScoreColumnTest,FtsMatchPushdownTest' test
```

（具体类名按实现后的版本模块命名调整；Base 类在 base 模块，版本模块只放空子类。）

### 9.2 集群专项（必须等 10M 五组窗口空闲，先申请）

基线合同不变：固定 snapshot/索引/参数（`base_tokenizer=simple`、`language=English`、`with_position=true` 等），BM25 专项单独出表，不替换五组向量。

| 场景 | 基线（现状） | 新实现 | 记录 |
|---|---|---|---|
| 10M BM25 k=10，无 residual（`WHERE lance_match(body,'x') ORDER BY _score DESC LIMIT 10`） | 现状 k 下推，单页 | 分页关闭（应等价） | p50/p95、plan、requests、bytes、rows、cache |
| 10M BM25 k=10，`_score > 0` residual | k=MAX_VALUE，全量 IPC | 分页/回退 | 同上 + 峰值堆、IPC bytes、是否 OOM |
| 10M BM25 k=10，非下推 residual（`MOD(id,2)=1`），先做 A 再测 | k=MAX_VALUE | A 下推后应变成无 residual | 同上 + residual 是否消失 |
| 10M `WHERE lance_match(...)` 无 LIMIT | 全量返回 | 分页耗尽 | 总行数、总 bytes、峰值、耗时 |
| 高选择性 term（命中 < 1000） | 全量返回 | 分页 1 页 | 请求数、延迟 |

验收门槛（建议，最终由 #3262 复核）：I1–I7 全部保持；residual 场景不 OOM、单 task 峰值堆 < 容器预算的 50%；无 residual 的 k=10 延迟不回退超过 5%；分页请求数 ≤ ceil(匹配数/pageSize)+1；测试/报告保留失败与负收益。

### 9.3 与 10M 五组的关系

按伞卡合同，每个大 feature 合入前要重跑五组向量基准并记录相对原始基线/上一个 feature；FTS 改动不应改变向量路径的计划（`shouldNamespaceFtsScan` 只对 fullTextQuery 生效），但仍要跑五组回归证明无副作用。FTS 专项的 BM25 k=10 是额外一组，不参与五组的比值表。

## 10. #3264 / #3265 的前提、依赖与风险

### 10.1 #3264（local 一组 fragments 共享 native scan / 全局 scorer）

前提（代码可证）：

- Java 侧 `ScanOptions.fragmentIds(List<Integer>)` 存在，`Dataset.newScan(ScanOptions)` 可一次扫多个 fragment；`Fragment.newScan` 会强制覆盖成单 fragment（`J:org.lance.ipc.ScanOptions$Builder#fragmentIds`、`J:org.lance.Dataset#newScan(ScanOptions)`、`J:.../Fragment.java:99-106`）。仓库里 `LanceCountStarPartitionReader` 已经在用 `dataset.newScan(fragmentIds=[...])` 做多 fragment 的 count（`L:.../read/LanceCountStarPartitionReader.java:60-118`），说明这条 API 不是纸面能力。
- native 的 FTS 扫描在打开全部分段时用 `IndexBM25Scorer` 汇总**已加载分区**的 `num_docs/total_tokens`（`R:.../scorer.rs:264-300`），并用 `fragments_covered_by_fts_query` 把结果 mask 到目标 fragment（`R:.../scanner.rs:5005-5018`）。因此“把一组 fragment 放进一次 scan”仍然用全语料统计，前提是**仍然打开全部 committed segments**。

风险：

- 如果为了省 segment open 而只打开覆盖本组的 segment，`IndexBM25Scorer` 的 N/avgdl/df 会变成局部的，分数不可比（这正是 #3265 要解决的问题）；#3264 不能顺手做这个优化。
- 分组后 `limit` 是“每组 top-k”，全局 top-k 需要在 Spark 侧合并；只有在 Spark 保留全局 Sort 时正确（同 §4.3）。分组粒度、每 task 内存（一组 fragment 的全部命中）、并行度都要重新测。
- `_score`/`_rowid` 列、`prefilter`、`whereCondition`、branch/tag、删除行/overlay 的语义要逐项回归；分批读取的取消/重试边界要有测试。

### 10.2 #3265（按共享全语料统计做分布式 FTS）

前提（代码可证）：

- BM25 的 N/df/avgdl 是全局量；native 的 scorer 统计来自加载的 index partitions（`R:.../scorer.rs:170-200,264-300`）。如果各 worker 只加载自己 fragment 对应的 posting/partition，各自算 IDF，就会“独立 local IDF 混排”，排名错误——这与分析文档的结论一致（`docs/analysis/lance-spark-search-optimization-analysis.md:99-105`）。
- Java 14.0.3 **没有**注入统计/全局 scorer 的 API：`ScanOptions` 没有 stats/scorer 字段，`FullTextQuery` 没有，`Dataset` 只有 `getIndexStatistics`（读静态覆盖信息）；`PreparedBm25Query`/`HybridCompoundQueryExec.with_indexed_input` 是 Rust 内部能力（`R:rust/lance/src/io/exec/fts.rs:1077-1083`），未通过 JNI/Java 暴露。
- 所以 #3265 需要 core 扩展（最小方案：暴露“给定 segments 列表的全局统计”或“注入 scorer/prepared query”的 Java API），先出接口评审，再实现；不能在 connector 里自造 BM25。

风险：

- 统计注入必须与分词器、位置索引、fuzzy 扩展一致；不同 segments 的 committed 集合必须在 driver 侧固定并传给 worker，避免同一 job 内 task 看到不同索引快照。
- 如果 core 只能给“统计”而不能给“按 posting 范围搜索”，worker 仍会加载全部分段，收益有限；需要同时评审 segment 选择 + 统计注入的组合。
- 任何 score 改动都要保留与旧路径的 A/B 对照；不能把“看起来更快”当作正确性证据。

## 11. 证据索引（便于复核）

### 11.1 本仓库（dev `5cbafef`）

| 主题 | 位置 |
|---|---|
| 版本 | `pom.xml:55-58` |
| 函数注册 | `lance-spark-base_2.12/src/main/java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java:311-357` |
| FTS 规则 | `lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/optimizer/AbstractLanceFtsPredicateRule.scala:65-108,168-330`；`lance-spark-3.5_2.12/src/main/scala/org/apache/spark/sql/catalyst/optimizer/LanceFtsPredicateRule.scala:17-28` |
| 表函数/扩展 | `lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala:159-213,236-321,355-362`；`lance-spark-3.5_2.12/src/main/scala/org/lance/spark/extensions/LanceSparkSessionExtensions.scala:41,46-64` |
| read options | `lance-spark-base_2.12/src/main/java/org/lance/spark/LanceSparkReadOptions.java:67,274,679-681` |
| 路径选择/namespace 扫描 | `lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java:157-160,209-223,600-660,701-748,802-804` |
| local 切分/reader | `.../read/LanceSplit.java:92-103`；`.../read/LanceScan.java:453-507,623-631,1081-1121`；`.../internal/LanceFragmentScanner.java:77-135,310-312` |
| namespace reader | `.../search/LanceSearchScan.java:24-53`；`.../search/LanceSearchColumnarPartitionReader.java:80-95`；`.../search/LanceSearchQuery.java:176-181,281-355` |
| hybrid | `.../search/LanceHybridSearchRowPartitionReader.java:55-120` |
| 观测 | `.../read/metric/LanceCustomMetrics.java:40-48,198-210`；`.../read/metric/LanceReadMetricsTracker.java:206-216` |
| _score 契约 | `.../LanceDataset.java:166-200`；`.../utils/Utils.java:42,53,56-62` |
| 过滤下推 | `.../read/FilterPushDown.java:60-121` |
| COUNT(*) | `.../read/LanceCountStarPartitionReader.java:60-118` |
| 测试 | `.../src/test/java/org/lance/spark/read/BaseFts*.java`、`.../search/BaseSparkSearchTableFunctionTest.java`、`.../utils/FullTextQuery*Test.java` |

### 11.2 lance-core 14.0.3（Java API）

- `org.lance.ipc.ScanOptions` / `Builder`：`fragmentIds`、`fullTextQuery`、`prefilter`、`limit`、`offset`、`collectStats`、`disableScoringAutoprojection` 等字段/方法。
- `org.lance.ipc.ScanStats`：`iops`、`requests`、`bytesRead`、`indicesLoaded`、`partsLoaded`、`indexComparisons`、`indexCacheHits`、`indexCacheMisses`、`allCounts`、`allTimes`。
- `org.lance.ipc.FullTextQuery`：`match/phrase/multiMatch/combinedFields/boost/booleanQuery`，默认 `boost=1.0`、`maxExpansions=50`、`operator=OR`、`prefixLength=0`；`DocumentGranularity`（ROW/LIST_ELEMENT）；`Operator`（AND/OR）。
- `org.lance.Fragment#newScan(ScanOptions)` 用单 fragment 覆盖 `fragmentIds`；`org.lance.Dataset#newScan(ScanOptions)` 接受多 fragment；`Dataset#getIndexStatistics(String)`。
- `org.lance.namespace.model.QueryTableRequest/QueryTableResponse`：`k/offset/filter/prefilter/columns/with_row_id/version/full_text_query`；响应 `data`（Arrow IPC bytes）+ `context`（Map，连接器未使用）。
- sources jar（本次只读下载）：`/tmp/lance-core-14.0.3-sources.jar`，sha256 `b194810527593f7d1987e485b961f41cac287a5984d2fe2f1b76fe0d26050436`；`javap` 对应 jar：`~/.m2/repository/org/lance/lance-core/14.0.3/lance-core-14.0.3.jar`。

### 11.3 Lance 上游 v14.0.3（Rust，只读拉取）

| 主题 | 位置 |
|---|---|
| BM25 参数/公式 | `rust/lance-index/src/scalar/inverted/scorer.rs:88-105,158-200,236-247,264-360` |
| top-k limit / 堆 | `rust/lance-index/src/scalar/inverted/index/search.rs:761-766,883-909` |
| ScoredDoc 排序（仅 score） | `rust/lance-index/src/scalar/inverted/builder.rs:1975-2010` |
| 参数/自动模糊 | `rust/lance-index/src/scalar/inverted/query.rs:12-20,56-96` |
| 计划入口/排序/limit | `rust/lance/src/dataset/scanner.rs:2862-2905,2919-2960,3915-3918,4505-4533,5005-5018,5326-5338,5603-5762,6163-6195,6270-6298,8547-8554` |
| exec 契约/合并/exactness | `rust/lance/src/io/exec/fts.rs:305-370,1077-1083,1402-1445,3029-3240` |
| index statistics | `rust/lance/src/index.rs:3303-3320,3607-3651` |

Java 侧复核命令（无需联网）：

```bash
JAR=~/.m2/repository/org/lance/lance-core/14.0.3/lance-core-14.0.3.jar
javap -classpath "$JAR" org.lance.ipc.ScanOptions org.lance.ipc.ScanStats \
  org.lance.ipc.FullTextQuery org.lance.DocumentGranularity
javap -classpath "$JAR" org.lance.namespace.model.QueryTableRequest \
  org.lance.namespace.model.QueryTableResponse
javap -classpath "$JAR" org.lance.Dataset org.lance.Fragment | grep newScan
```

Rust 拉取命令（可复核）：

```bash
TAG=v14.0.3
BASE=https://raw.githubusercontent.com/lance-format/lance/$TAG
curl -sSL -o /tmp/lance-src/scanner.rs $BASE/rust/lance/src/dataset/scanner.rs
curl -sSL -o /tmp/lance-src/fts_exec.rs $BASE/rust/lance/src/io/exec/fts.rs
curl -sSL -o /tmp/lance-src/builder.rs $BASE/rust/lance-index/src/scalar/inverted/builder.rs
curl -sSL -o /tmp/lance-src/scorer.rs $BASE/rust/lance-index/src/scalar/inverted/scorer.rs
curl -sSL -o /tmp/lance-src/query.rs $BASE/rust/lance-index/src/scalar/inverted/query.rs
curl -sSL -o /tmp/lance-src/search.rs $BASE/rust/lance-index/src/scalar/inverted/index/search.rs
```

## 12. 交给 #3263 的最小任务分解（建议 PR 顺序）

1. PR-1（只读能力/测试）：U1/U2/U3 的离线用例 + 失败基线（TDD 红），确认分页/全序/Spark 计划假设；不改产品行为。
2. PR-2（A）：`FilterPushDown` 白名单扩展 + 差分等价测试；residual 比例下降的取证（同一组查询的 `hasResidualPredicates` 前后统计）。
3. PR-3（B）：`LanceSearchQuery` 重载 + paged reader + 配置 + `LanceSearchScan` metadata/metrics；默认 `paging.enabled=false`，测试开启。
4. PR-4（D）：local metrics 补映射（`indexCacheHits/Misses`）、`LanceSearchScan.description/getMetaData`；`_score`/`ORDER BY _score DESC, _rowid ASC` 的确定性测试。
5. PR-5（可选 C）：local candidate hint（默认关）+ plan/正确性测试。
6. 文档：`docs/src/operations/dql/fts.md` 补 BM25 公式、multi_match 打分、namespace/local 指标与分页/offset 语义；`docs/src/operations/ddl/create-index.md` 补 tokenizer/analyzer/document granularity 全量选项。

每步都遵守伞卡合同：TDD 先红后绿、只 add 具体路径、独立 worktree、合入前真实 smoke、完成后按原参数重跑 10M 五组（向量）并另附 BM25 k=10 专项；负收益/失败如实记录。

## 13. 本次调研的边界与未做

- 没有提交任何 Spark/K8s 作业，没有 ssh 到集群，没有改 `dev`/`sync-091` 或产品代码。
- 没有运行本地 Spark 用例（只做静态核实）；所有“行为”判断来自代码/测试断言/文档，标了证据；需要运行才能确认的列在 §7。
- 没有验证 namespace REST/directory 的实际 plan/metrics（`queryTable` 是 native 方法，Java 侧不可见）；#3262 的集群侧要把 connect/request/IPC 计时补上。
- `wand_factor`、`fast_search` 对 FTS 的影响、`combined_fields` 的 connector 支持不在本卡范围；已在 §3/§7 标为后续项。
