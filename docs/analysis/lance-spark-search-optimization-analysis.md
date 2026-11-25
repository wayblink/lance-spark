# lance-spark 检索优化分析：以 dev 为开发基线

日期：2026-11-25；卡片：#3248。开发与集成目标为 `dev`，本次审查版本 `b102cc78d5ba0cda54a0905ad62c3f4bd93a2980`：connector `0.9.1-beta.1`、Lance Core `14.0.3`、namespace `0.12.0`，默认 Spark `3.5.5`。版本依据：[pom.xml:54–75](../../pom.xml#L54-L75)。本报告取代早前以 `main d55b2e8 / Lance 12` 为实现基线的建议；旧 R3/R2 数字仅作为历史线索，不代表 dev 的新成绩。

## 1. 结论

**lance-spark 仍有明显优化空间，但以 dev 为准，重点已不是“增加批量入口、增加扫描指标、从零造缓存”，而是减少搜索过程中不必要的取列、局部结果回表、重复打开和大批量归并工作。** 已有事实不足以证明 Lance 的原生 IVF 或 BM25 内核本身比 iceberg-index 差；也不能承诺下面每个改动都能带来固定倍数。

推荐分两条线推进：向量先做自动列裁剪和已有指标的有效取证，再做只传 rowid/score、全局选完才取 payload，随后才是批量内存预算、打开复用和归并。文本先确保正确走 ranked top-k，减少 residual 引发的全匹配输出；local 路径按一组 fragments 共享一次 native scan/scorer，长期再做共享全语料统计的分布式 FTS。保留 Lance 原生索引格式与算法，不在 connector 重写一套 IVF/BM25。

本轮做了源码、现有 JAR 接口/字节码和历史材料复核，**没有构建、启动 Spark、跑新 EXPLAIN、提交集群作业或执行性能测试**。因此下文严格区分代码事实、静态工作量推导和待验证方案；没有 dev 新延迟数据。

## 2. dev 已有能力：不能再当作待开发项

| 能力 | 已确认实现 | 剩下的问题 |
|---|---|---|
| 真批量向量入口 | `queries` / `query_table`；一个 native scan 使用 `Query.setKeys` 回答整批，输出 query id；只支持 distributed | 无分块/内存预算，query table 在分析期全量收集；批量归并与宽 payload 仍可能很贵 |
| 搜索扫描指标 | Scan 声明 custom metrics，reader 记录 Dataset open、scanner create、batch load、IO/比较次数/rows_scanned | 不含 driver 规划分解；尚未映射 core 已有 index-cache hit/miss；namespace queryTable 没等价指标 |
| 原生 index/metadata cache | 每 executor JVM 内按 catalog 共享 Session，支持 backend URI、容量和程序化注册 | 预算/实际命中/驻留分布缺充分证据；每个搜索 reader 仍独立 open/close Dataset |
| 指定索引 | distributed 支持 `index => 'name'`，不会静默换索引 | 每次新计划仍做索引发现；应按版本优化轻量元数据读取 |
| multi-match 语义说明 | 已明确“任意列匹配；AND 是同一列包含所有 query terms” | 旧 skip 和部分弱测试仍待与 core14 能力对齐，不再把旧文档错误算作 dev 缺陷 |

证据：[批量调用与 collectStats](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L174-L261)、[Scan metrics](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L106-L121)、[catalog Session/cache](../../lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L242-L339)、[现有缓存配置](../src/config.md#cache-backends)、[multi-match](../src/operations/dql/fts.md#lance_multi_match)。

## 3. 向量查询：最值得改的是哪里

### 3.1 自动列裁剪缺失，是已证实且风险较小的第一项

`LanceDistributedSearchScanBuilder` 与 namespace 的 `LanceSearchScanBuilder` 目前都只实现 `ScanBuilder`，没有 `SupportsPushDownRequiredColumns`。TVF 的 `columns` 未指定时，schema 是全表字段；reader 只使用分析期已写入的 `query.outputColumns`。所以外层 `SELECT id, _distance FROM VECTOR_SEARCH(...)` 能减少 Spark 后续传递的列，**不能阻止 native scan 先读取 TVF 默认全列**。显式 `columns => array('id')` 已能规避，不能说所有查询都读全列。

证据：[distributed builder](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScanBuilder.java#L20-L32)、[namespace builder](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceSearchScanBuilder.java#L20-L32)、[TVF 默认全列](../../lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L774-L862)、[native columns](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L249-L260)。Spark 3.5.7 本地源码 `PushDownUtils.pruneColumns` 的 fallback 也明确：未实现该接口时保留 relation 全输出。

推荐让两个 search builder 接收实际 required schema，同步缩小 native output columns；保留排序必须的 `_distance/_rowid`，批量还要保留 `query_index`。`filter` 引用列和 native refine 所需向量是内部读取依赖，不等于必须出现在最终 Arrow 输出。不要通过改变 `SELECT *` 的含义“优化默认行为”。

最小验收：同一 query 的显式 columns 与外层窄 SELECT 返回相同 id、distance、tie 顺序；reader 的 native output 确实不带未用向量/正文；`SELECT *` 不变；覆盖单查、批量、namespace/distributed、rowid-only 和混合 indexed/flat。仅看到外层 Project 变窄不算通过。

### 3.2 全局选完再取 payload：收益上限比换排序算法更大

当前每个 segment 都在 native nearest scan 内完成本地检索、可选 refine、请求列回表，返回本地 top-k，然后 Spark 才选全局 top-k。假设段数 U、query 数 B、k，native 输出最多约 `U × B × k` 行，而最终只要 `B × k` 行。这里放大的是 payload 读取与输出，不等于所有索引候选数量，也不保证实际每段都返回满 k。

以 317 段、2560 维 float32 为例，只计算返回原向量这一列的未压缩数值字节：

| B | k | 局部结果上界 | 原向量列上界 | query_id + rowid + float score 的最低数值字节 |
|---:|---:|---:|---:|---:|
| 1 | 10 | 3,170 行 | 30.96 MiB | 49.53 KiB |
| 100 | 100 | 3,170,000 行 | 30.23 GiB | 48.37 MiB |
| 100 | 2000 | 63,400,000 行 | 604.63 GiB | 0.945 GiB |

这是算术工作量上界，不是测得的 OSS、shuffle 或 RSS；未计压缩、Arrow、字符串、对象头和精排读取，也未计 Spark 在 source 之后可能消除的局部结果。它说明：**批量入口已经存在，并不意味着宽列回表成本已被摊薄。**

推荐分两阶段：各段只输出 `query_index/_rowid/_distance` → 保留现有排名合同选全局赢家 → 按相同 snapshot 对赢家取 payload。Lance14 已有 `Dataset.takeRows(List<Long>, List<String>)`，可先做 connector 原型，不必默认等待新增 JNI 接口。

但不能把它当无风险快路：本机 core14 JAR 字节码表明 `takeRows` 的 native 返回先是完整 `byte[]`，再包装成 ArrowReader，因此必须分块；还需实测输入顺序、重复/缺失 rowid、删除和 stable row id 行为，不能靠数组位置猜映射。候选层保留 query id、rowid、distance 和 rank，回表只取普通 payload 列，并固定搜索版本而不是打开 latest。

**正确性红线：只延迟 payload，不把原有每段 overfetch/refine 偷换成全局截 k 后再 refine。** 后者可能漏掉量化分数暂时靠后、精排后能胜出的行，改变答案。两阶段还会多一次调度和 Dataset open；窄列、小段、小 k 时可能不划算，宜按预计 payload 成本选择，而不是全量替换。

### 3.3 批量已有，但目前是无预算的大批次

[readQueryTable](../../lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L610-L650)在 TVF 分析期执行 `spark.sql(...).toLocalIterator()`，把全部 query 放进 ArrayBuffer；指定 id 列只是排序，输出 query_id 是排序后的零基序号，不保留原业务 id。NULL 向量会被跳过；重复排序键不保证业务上的稳定归属。小查询表能用，但不是可无限扩展的分布式 query relation。

查询矩阵还经历多份表示：driver 的嵌套 boxed List → 扁平 List，随 partition 传递 → 每 task 构造 `float[B][D]` → core `setKeys` 再展平为 `float[B×D]`。这不是当前最大成本的证据，但大 B/D 会增加堆和序列化压力。[矩阵构造](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L288-L305)、[Builder 展平](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceSearchQuery.java#L501-L531)。

推荐先明确 query id/NULL 契约，再加输入向量字节、预计 `U×B×k×行宽`、native scratch、shuffle 的预算，按预算分块；广播/flat SDK 入口可减少矩阵复制。分块过小会重复规划/open、增加作业数，不能以“内存小了”直接宣布更快。core14 已有 `setQueryParallelism`，connector 尚未暴露；要与 Spark task slots 联合调节，不盲目开大 native 并行度。

### 3.4 归并要按 k 分析：Spark 已有部分优化

有效批量逻辑是 `Window(row_number, partition=query_index, order=distance,rowid) + rank<=k`。本地 Spark 3.5.5/3.5.7 的规则与 SQLConf 字节码确认：默认 `spark.sql.optimizer.windowGroupLimitThreshold=1000`，该形状在 k=10/100 时具备插入 Partial/Final WindowGroupLimit 的条件；k=2000 默认不触发。最终是否生效仍取决于配置、被排除规则和实际物理计划；本轮未运行 EXPLAIN。

即使触发，也仍要按 query 分布和排序；一个 source task 已经只为每 query 返回 k 行时，Partial 通常无更多行可裁。它不能撤销 native 已完成的 local refine/回表。单查询 `Sort+Limit` 也可能已是 TakeOrdered，不应先写一套重复的 top-k 执行器。

建议先记录实际 plan、exchange bytes、spill、sort/merge 时间；确认 k=2000 的 Window 是瓶颈后，再做有界的分组 top-k。不能把每段返回数量设成 `k/U`：全局最好的 k 行可能全在同一个段。dev 还留有一个无调用点的旧 `batchRelation` 四参数重载，应作为清理项；不能用死代码里的错误 limit 推断当前结果为空。[当前有效路径与旧重载](../../lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L418-L534)。

### 3.5 重复 open、缓存和 locality 应一起看

driver 每个新 scan 仍 open Dataset、describeIndices、解析 coverage/metric；worker 每个 segment task 仍 open Dataset/create scanner/close。共享 Session 和 index/metadata cache 已存在，**关闭 Dataset 不等于清空缓存，不能推断每次都重新读取整份索引**。搜索 partition 没提供自定义 preferred locations，也没有索引段驻留位置反馈，重复查询不保证落到持有相同段缓存的 executor。

证据：[driver 规划](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L123-L239)、[昂贵 statistics 的 fallback](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L375-L540)、[worker open](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L174-L195)、[partition](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchInputPartition.java#L24-L80)、[共享 Session 注入](../../lance-spark-base_2.12/src/main/java/org/lance/spark/utils/Utils.java#L208-L242)。

先转发 core14 `ScanStats.getIndexCacheHits/Misses`，结合有效 cache budget、bytes/parts、task 所在 executor、GC/native memory 取证；driver 加 planning/discovery 时间。`batchLoadTimeNs` 包含 native 检索、IO和回表，**不是纯 JNI 跨界开销**；task 时间总和也不是用户端墙钟。

复用方案优先有界的 Dataset lease（catalog/身份/URI/pinned version 组成 key），只共享只读 Dataset，scanner 仍每次新建；验证关闭、取消、凭证刷新和并发。先量 open 占比，不能凭一个 open 调用承诺大收益。分组任务也需保留原每段候选/refine：在同一 Dataset 下顺序搜多段更易保语义；直接把多 UUID 交给一个 native scan，可能改变 refine/候选切分，应另验。过度 coalesce 会降低并行度、增加尾任务，不能以 task 更少作为唯一目标。

## 4. 文本查询：不能只复制向量的分片方式

### 4.1 namespace ranked top-k 与 residual 的成本差别巨大

dev 的 SEARCH 与 FTS 谓词汇入共享 `LanceScanBuilder`。有 FTS、namespace override queryTable、非 branch/tag、无 pushed aggregation 才走单分区 namespace；单 Spark task 不表示 native 单线程，更不表示 REST 服务只能用一个核。[选择条件](../../lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java#L600-L609)。

该路径已有普通列裁剪，不能把向量 TVF 的缺口套到 SEARCH。真正放大点是残余谓词：单 `_score DESC`、无 residual 才能下推 ranked top-k；有 residual 或没有可用 limit 时请求 `k=Integer.MAX_VALUE`，先取全匹配，再在 Spark 过滤/截断。[top-k 与 residual](../../lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java#L616-L657)、[pushTopN](../../lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java#L732-L746)。namespace reader 又先取完整 IPC `byte[]`，因此常见词加 residual 可同时放大打分、回表、编码和 Java 堆。[reader](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceSearchColumnarPartitionReader.java#L79-L95)。

先扩展有严格等价语义的标量下推；其他 residual 可考虑固定版本的有序候选分批与 continuation。**不能简单把 MAX_VALUE 改成 k**，否则前 k 行被过滤掉后会漏掉真正满足条件的后续行。WHERE match 无 LIMIT 要求返回全部命中，本来就不能用 ranked top-k 替代。score residual 和多键排序也应分别证明可下推性，不能统称为“都能优化掉”。

### 4.2 local 路径重复初始化：要减少 native scan 数，不仅是 task 数

catalog-only namespace、branch/tag 等会落到 local fragment scan。每个 fragment 创建 Dataset/scanner；该路径无 ranked top-k 下推，先输出全部命中交给 Spark 排序。dev 文档给出的成本是 `N_fragments × M_FTS_segments` 的 scorer/index-open：每个 fragment scan 为完整语料 scorer 涉及全部 committed segments。这是重复 setup 的成本模型，不是每次都冷读 OSS 的证明。[local scanner](../../lance-spark-base_2.12/src/main/java/org/lance/spark/internal/LanceFragmentScanner.java#L68-L136)、[FTS 成本说明](../src/operations/dql/fts.md#cost-model)。

可尝试一次 `Dataset.newScan(fragmentIds(List), fullTextQuery, prefilter)` 搜索一组 fragments，减少 scorer 初始化。仅把多 fragment 塞进 Spark split、reader 内仍逐 fragment.newScan，只省调度，不省 native scorer。组内 limit/top-k、score 是否与当前全语料一致，需要 core 合同和小表精确答案验证；已有 SDK 接口不等于已证明这个组合正确。

### 4.3 分布式 BM25 的关键约束是全局统计

向量距离可以跨段比较，BM25 若用不同段的 `N/df/avgdl` 则不行。比如一个词在 A 段很罕见、B 段很常见，独立计算 IDF 会把 A 的命中虚假抬高；简单合并 local top-k score 会得出错误排名。

正确方向是固定完整覆盖的 snapshot，准备共享全语料统计，再让各 worker 搜索自己的 posting 范围、用同一把尺子打分，最后归并。若 core 不暴露共享 scorer/statistics 或搜索范围，connector 无法仅靠多个 index segment task 正确完成。优先复用 Lance 的全局 scorer，不在 Spark 重写分词和 BM25。

dev 已明确 core14 部分覆盖时 optimize 前后可能改变 score/rank；升级到14也可能改变停用词处理。因此不能用不同分词器、不同索引覆盖或不同 tokenization 结果的两边延迟宣布内核优劣。[覆盖与升级限制](../src/operations/dql/fts.md#relevance-score-_score)。

### 4.4 需要修的是能力验收，不是沿用旧版本结论

`supportsQueryTable` 只反射方法是否 override，没有 structured/score/top-k 能力协商。dev 仍有写着旧12限制的 skip/Disabled、容忍全表 superset 的 phrase 用例；同时存在调用真实 DirectoryNamespace、检查精确 id/score/k 的未禁用测试。**未跑 core14 smoke 前，不能宣布 structured FTS 一定坏或一定好**。multi-match 的公开 AND 语义在 dev 已修正，不再重复报缺陷。

下一轮最小 smoke 应验证 match/phrase（含不存在的词和空结果）、multi-match 列内 AND、score、scalar filter、score residual、limit、requested columns；分别确认 namespace 与 local route 的实际计划和精确集合。[能力判断](../../lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L152-L186)、[严格 top-N 测试](../../lance-spark-base_2.12/src/test/java/org/lance/spark/read/BaseFtsScoreColumnTest.java#L166-L237)、[multi-match 语义](../src/operations/dql/fts.md#lance_multi_match)。

### 4.5 namespace 与 hybrid 的生命周期还可优化

`getOrCreateNamespace` 实际每次 connect；namespace reader 执行 queryTable 后关闭，没有显式把 `LanceRuntime` 的 catalog Session 注入这次请求。不能据此说 server 没缓存，但也不能把 Dataset 路径配置的 index cache 预算当作 namespace 实际预算。应观测 connect/request/IPC，按 backend 能力决定复用方式。

hybrid 在一个 task 中顺序做两次 queryTable，两侧都提前取 payload，逐行 copy 成 List/Map 后 RRF 排序。可先两侧只取 rowid 和指标、融合后去重回表；只为最终赢家读一次 payload。并行发双请求可能减少墙钟，也可能争抢CPU/IO；必须固定同一版本，并保留 candidates 对融合召回的限制。[hybrid reader](../../lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceHybridSearchRowPartitionReader.java#L61-L148)。

## 5. iceberg-index 的优势与不能过度外推的部分

对照的 iceberg-index 使用自有 IVF/RaBitQ 和 BM25 sidecar，Lance 主要用于按位置读数据；不是同一 Lance native 内核的另一种 Spark 包装。它已具备显式驻留、位置反馈、批量 task 复用和部分场景全局选候选后回表。lance-spark 则保有 Arrow 列式输出、Lance 原生索引与维护、无需自建两套算法的一体化优势；大 k 时历史数据也并非全面落后。

原报告引用的 R3/R2 仅作线索：[R3 标准表](</Users/jyxc-dz-0101035/workspace/iceberg-tools-wt/3233/docs/r3-standard-table-2026-11-24.md>)、[BM25 R2](</Users/jyxc-dz-0101035/workspace/iceberg-tools-wt/3233/docs/bm25-full-2026-11-24-r2.md>)。两者的镜像、入口、回表列、缓存预算和召回并非都与本次 dev 对齐；部分10M高k truth不完整。本文不重新给跨侧倍数，不把旧镜像“无批量入口”当作 dev 现状，不把一侧总时间/某个任务时间当作 Lance 算法的独立耗时。

也不建议照搬对方全部实现：iceberg 文本缓存目前主要存编码 payload，暖查询仍可能解压 doc-length/postings、建立对象和 score map；某些 CALL 还需要 JSON/driver 物化。优化 lance-spark 应借鉴“少做工作”的执行形态，而非照搬自有格式或认为 Java 天然优于 Rust。

## 6. 推荐施工顺序及最小验收

| 顺序 | 推荐工作 | 归属与收益条件 | 关键不变量/验收 |
|---|---|---|---|
| 1 | 向量 TVF 自动 required-column pushdown | connector；宽表、外层窄 SELECT 最受益 | id/distance/tie 不变；native 输出确实少列；SELECT * 不变 |
| 1，并行 | 复用已有 metrics，补 cache hit/miss 和 driver planning | connector 可先做，不是重建 metrics | 计数不重复；task sum 不冒充墙钟；batchLoad 不叫纯JNI |
| 2 | 仅延迟 payload 的两阶段回表原型 | connector可用takeRows；宽列、多段、大批次 | 同snapshot；不变local refine候选；重复/缺失rowid映射正确；有界IPC |
| 2 | 批量分块与query-id/NULL合同 | connector；大B/D/k需要内存边界 | 每query独立k；跨块答案一致；OOM前显式拒绝或分块；不自动放大并行 |
| 3 | 有界Dataset lease、轻量metadata与locality | connector+按native缓存合同验证；open/miss占主导才高收益 | catalog/身份/版本隔离；凭证更新；取消/淘汰不关闭在用对象 |
| 3 | k分段的Window/top-k优化 | 先EXPLAIN与spill取证，再做connector；重点高k | 不遗漏集中在一段的赢家；保留query归属和tie |
| 文本1 | core14严格smoke、ranked/residual指标 | 先确认语义，不需改算法 | 不允许superset冒充正确；完整覆盖和统计固定 |
| 文本2 | residual有界执行、grouped-fragment native scan | connector与core共同验证 | residual不截漏；不同分片使用同一BM25统计 |
| 文本3 | hybrid候选融合后回表 | connector原型；候选宽/双侧重合时收益更大 | 同版本；RRF合同与候选截断不变 |

不推荐起步就引入独立服务或重写内核。它们增加部署、版本和维护成本；只有上述工作收敛后，现有 Spark 固定开销仍达不到明确业务目标时，再讨论在线常驻服务。

**第一张实现卡推荐做“向量搜索自动列裁剪”，而不是重做 batch/cache。** 它证据明确、接口现成、最容易用保持答案不变的回归证明收益；但本报告不是实现授权，也没有新性能承诺。需要集群提交、镜像构建或长 benchmark 时，仍须先说明耗时与风险再执行。
