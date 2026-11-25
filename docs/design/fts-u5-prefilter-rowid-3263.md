# U5 本地实证与改动 A（#3263 第一阶段）

本卡（#3263）第一阶段的两件事：用 `DirectoryNamespace` + 本地 Spark 实证 U5（`_rowid` 能否作为 `queryTable` 过滤列、`prefilter=true` 是否严格"先过滤再排序"、`k=0`、rowid 稳定性），以及在 `FilterPushDown` 中收紧/扩展可翻译为 Lance SQL 的表达式白名单（改动 A）。结论先说：

- **U5-1 `_rowid` 可过滤：确认**。范围（`>=`/`<`）、`IN`、单值都可用，过滤真实生效（越界范围返回 0 行，精确范围逐行相等）。
- **U5-2 `prefilter=true` 严格先过滤后排序：确认**。同一份数据下 `prefilter=true` 返回"过滤后子集"的精确 top-k；`prefilter=false` 先取全局 top-k 再过滤（可返回 0 行）。
- **U5-3 `k=0` 仍表示"不限制"：确认**（无过滤 25/25；带过滤返回全部命中过滤的行）。
- **U5-4 rowid 稳定性：append 后旧行 rowid 不变、部分索引不影响 rowid，但 `OPTIMIZE`（compaction）会重写全部 rowid**。所以 B' 只能在同一 pinned snapshot 内使用 rowid 范围。
- **改动 A**：新增 `<>`、`STARTS_WITH`/`ENDS_WITH`/`CONTAINS`（改写为带转义的 `LIKE`）、`LIKE`（手工谓词）、`UPPER`/`LOWER`/`CHAR_LENGTH`、`+ - * %` 的严格等价下推；排除 `/`（Spark 整除返回 double，而 Lance/DataFusion 保持整数除法）。用同一数据集 pushdown on/off 逐行差分，全部新映射结果一致；FTS 场景下可把 `k` 从 `2147483647` 收到真实 LIMIT（实测 `k=5`）。
- **B' 判断**：正确性前提已具备（U5-1/2/3 成立，且每段 top-k 精确），但**还不能直接启用**：需要显式 pin version（防 `OPTIMIZE`/并发写改号）、处理稳定 rowid 表的大 u64 字面量、并在 10M 上测每段 mask 的代价。

## 1. 结论表

| 前提 | 结论 | 一句话依据 |
|---|---|---|
| U5-1 `_rowid` 可作 `queryTable` 过滤列 | **确认** | range/IN/empty 三类过滤结果与 rowid 集合逐行相等（§3） |
| U5-1 过滤是否真正生效 | **确认** | 越过最大 rowid 的范围返回 0 行；IN 只返回列出的行（§3） |
| U5-2 `prefilter=true` 先过滤再排序 | **确认** | `id<20` + k=5 返回低分段的 [19,18,17,16,15]；`prefilter=false` 同查询返回 0 行（§4） |
| U5-3 `k=0` 表示不限制 | **确认** | 无过滤 25 行、带过滤 10 行、带 rowid 过滤 25 行（§5） |
| U5-4 append 后旧 rowid 是否变 | **不变** | ids 0..9 的 rowid 在追加前后完全相同（§6.1） |
| U5-4 partial index 是否影响 rowid/召回 | **不影响** | 覆盖 [0,1,2]/4 个 fragment 的索引仍返回 12/12 行（§6.2） |
| U5-4 compaction（OPTIMIZE）后 rowid | **全部改变** | 8→4 fragments，20/20 行 rowid 变号，旧范围返回 0 行（§6.3） |
| 稳定 rowid 表可否用范围过滤 | **可用（小值已验证）** | stable rowid {0..9}，range 与 IN 均精确（§6.4） |

## 2. 探针环境、命令与产物

- 工作树 `/Users/jyxc-dz-0101035/workspace/lance-spark-wt/3263`，分支 `card/3263`，基线 dev `93d5860`。
- 探针类：`lance-spark-base_2.12/src/test/java/org/lance/spark/read/BaseFtsU5ProbeTest.java`，Spark 3.5 绑定 `lance-spark-3.5_2.12/src/test/java/org/lance/spark/read/FtsU5ProbeTest.java`。8 个测试、`Tests run: 8, Failures: 0, Errors: 0`，首跑 6.44 s、热跑 1.35 s。
- 环境：macOS/aarch64，JDK 11.0.29，Spark 3.5.5，Scala 2.12，lance-core 14.0.3，lance-namespace 0.12.0，`local[4]`，AQE 关闭。
- 复跑：

```bash
cd /Users/jyxc-dz-0101035/workspace/lance-spark-wt/3263
./mvnw -s /tmp/dsh-mvn-settings.xml -q -pl lance-spark-3.5_2.12 -am \
  -Dspotless.skip=true -Dtest='FtsU5ProbeTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

探针与 #3282 的一样是**分支内取证**，不属于 dev 的长期合同：断言固定的是 14.0.3 的观测行为，复跑可发现漂移。

## 3. U5-1：`_rowid` 作为过滤列

事实（`PROBE| U5-1` 行）：

- `queryTable(withRowId=true)` 返回的 `_rowid` 列类型是 `Int(64, false)`（u64），Arrow Java 侧是 `java.lang.Long`；本次 40 行/8 fragments 的值域 `[0, 30064771076]`。值等于 row address：`fragment_id << 32 | row_offset`（U5-4 的 `_rowid==_rowaddr 20/20` 同证）。
- `filter="_rowid >= 0 AND _rowid < 12884901893"`、`filter="_rowid IN (0, 1, 2)"` 都被接受，且返回集合与"全量结果中 rowid 落在该范围的行"**逐行相等**。
- `filter="_rowid >= 30064772100"`（超过最大 rowid）返回 0 行——证明过滤没有被忽略（若忽略会返回全部 40 行）。
- 同一范围在 `prefilter=true` 与 `prefilter=false` 下都返回 20 行，说明 rowid 过滤在两条路径上都会被求值。

实现路径（源码，供复核）：`Scanner::filterable_schema_with_fts_granularity` 把 `_rowid`/`_rowaddr`/版本列加入可过滤 schema（`rust/lance/src/dataset/scanner.rs:2890`）；`prefilter_source` 用 `empty_projection().with_row_id()` 做 filtered read（同文件 `:8444`）。Lance 的 SQL planner 对 `_rowid =` / `IN` 还有 take 快路径（`dataset/scanner.rs:1400` 附近），范围走常规过滤。

## 4. U5-2：`prefilter=true` 是"先过滤再排序"

fixture：40 行，ids 0..19 的 tf=1..20（低分带），ids 20..39 的 tf=21..40（高分带）。全量排序后 global top5 = [39,38,37,36,35]，全部在过滤条件 `id < 20` 之外。

| 查询 | 结果 |
|---|---|
| `filter='id < 20', k=5, prefilter=true` | ids=[19,18,17,16,15]，scores=[0.02549261, 0.025473619, 0.02545255, 0.025429044, 0.02540265] |
| 同参数、`prefilter=false` | ids=[]（全局 top5 先被取出，再被过滤掉） |
| 全量 + Java 侧过滤后取前 5（真值） | 与 `prefilter=true` 的 ids 和 scores 完全一致 |
| `filter='_rowid >= 0 AND _rowid < 12884901893', k=5, prefilter=true` | ids=[19,18,17,16,15]，与上一行相同的 scores |

这说明：**在有界 rowid 范围 + `prefilter=true` 时，服务端给出的是"该范围内命中集"的精确 top-k**，不需要依赖同分顺序。U2 已证明分数层面精确；这里补上了"范围作为过滤条件"这一层。

## 5. U5-3：`k=0` 仍表示"不限制"

| 查询 | 行数 |
|---|---|
| `k=0`、无过滤 | 25（全部 match） |
| `k=0`、`filter='id < 10'` | 10（ids 0..9） |
| `k=0`、`filter='_rowid >= 0'` | 25 |

任何读取路径都必须防 `k=0`：它会把"有界读取"悄悄变成全量读取，不是"返回 0 行"。

## 6. U5-4：rowid 稳定性

### 6.1 append 后旧行不变

10 行/4 fragments → 追加 10 行 → 20 行/8 fragments。ids 0..9 的 rowid 前后完全相同（例如 `0=0, 1=1, 2=4294967296, ... 9=12884901890`）；追加行的 rowid 落在新 fragment 的地址段（`17179869184 = 4<<32` 起）。全量查询 20/20 命中，追加片段通过 flat 路径补分。

### 6.2 partial index / 追加后的未索引片段

- 追加发生在建索引之后：新 fragment 不在 FTS 索引覆盖内，但全量查询仍返回 20/20（flat 分支），只覆盖追加片段的 rowid 范围也精确返回那 10 行。
- 用原生 `IndexOptions.withFragmentIds` 建真正只覆盖 `[0,1,2]/4` fragments 的 FTS 索引：`queryTable` 仍返回 12/12 行，Spark 路径同样 12/12；rowid 未变。
- 结论：**索引覆盖不完整不影响 rowid 的含义与过滤**；影响的是"哪些行由索引分支/ flat 分支打分"。

### 6.3 compaction（OPTIMIZE）会重写全部 rowid

`OPTIMIZE <table>` 把 8 fragments 压成 4 个：20/20 行的 rowid 全部改变（例如 id=0 从 `0` 变为 `34359738368`），用压缩前的 rowid 范围查询返回 0 行。也就是说：**rowid 范围只对某个 snapshot 有效**，B' 必须把版本固定在同一 snapshot 上；不能跨 `OPTIMIZE`、rewrite 或并发写复用范围。

### 6.4 稳定 rowid 表

`enable_stable_row_ids=true` 的表：rowid 为 `{0..9}`（小值顺序分配），类型仍是 `Int(64,false)`/`Long`；范围 `_rowid >= 0 AND _rowid <= 9` 与 `IN` 都精确。注意本轮只测了小值；稳定 rowid 是任意 u64，**超过 `Long.MAX_VALUE` 的字面量表示尚未验证**（见 §8 未确认项）。

## 7. 改动 A：严格等价过滤下推

### 7.1 新增映射

`FilterPushDown`（`lance-spark-base_2.12/src/main/java/org/lance/spark/read/FilterPushDown.java`）的白名单现状：

| Spark V2 形状 | 渲染 | 差分结果 |
|---|---|---|
| `And` / `Or` / `Not` | `(...) AND/OR (...)`、`NOT (...)` | 已有，保持 |
| `=` `<` `<=` `>` `>=` | 二元比较 | 已有，保持 |
| `<>` | `!=` | 新增；注意 Spark 3.5 实际把 `id <> 3` 送成 `NOT (id == 3)`，`<>` 分支用于手工/未来形状 |
| `IS_NULL` / `IS_NOT_NULL` | `x IS [NOT] NULL` | 已有 |
| `IN` | `x IN (...)` | 已有 |
| `STARTS_WITH` / `ENDS_WITH` / `CONTAINS` | `x LIKE '<escaped>%' ESCAPE '\'` 等 | 新增，逐行等价 |
| `LIKE`（手工谓词） | `x LIKE '<pattern>'` | 新增；Spark 3.5/4.0 的 V2 builder 不产生该谓词（简单 LIKE 先被简化成 StartsWith/EndsWith/Contains，复杂 LIKE 留 residual），故此臂当前是惰性的 |
| `UPPER` / `LOWER` / `CHAR_LENGTH` | `upper(...)` / `lower(...)` / `char_length(...)` | 新增，含 ß/İ/连字/emoji 差分一致 |
| `+` `-` `*` `%` | `(a op b)` | 新增，仅在 Spark ANSI 模式下会由 V2 builder 产生 |
| `/` | **不下推** | Spark 整数 `/` 返回 double，DataFusion 保持整数除法，非严格等价 |
| 其他函数（`ABS`/`SUBSTRING`/...） | 不下推 | 保持 residual |

两个关键实现边界：

1. **LIKE 转义只接受反斜杠**。Lance 的 SQL planner 会拒绝其他转义符（实测报错：`LIKE does not support escape_char other than the backslash (\)`，`rust/lance-datafusion/src/planner.rs`）。因此 `STARTS_WITH`/`ENDS_WITH`/`CONTAINS` 生成 `ESCAPE '\'`，并把 needle 中的 `%`、`_`、`\` 分别转义为 `\%`、`\_`、`\\`；重写后的 needle 再加 `%` 锚定。
2. `And`/`Or` 的某一侧无法渲染时，**整个组合都不下推**；不能只丢一侧（那会把服务端过滤悄悄放宽）。`Not` 的子表达式无法渲染时同样整体留 residual。

### 7.2 差分测试

`BaseFilterPushDownEquivalenceTest`（`lance-spark-base_2.12/src/test/java/org/lance/spark/read/`，Spark 3.5 绑定 `FilterPushDownEquivalenceTest`）：同一份 Lance 数据、同一 SQL，分别以 pushdown on / `pushDownFilters=false` 读，要求 ids 逐行相等；对声称会下推的谓词还要求 scan metadata 的 `whereConditions` 出现预期 SQL，防止"没下推也算等价"。

fixture 含：null、空串、`abc`/`ABC`、`Straße`/`ß`、`İstanbul`、`ıI`、`a%b`、`a_b`、`a\b`、`😀emoji`、前后空格、`ﬁle`、`O'Brien`；负数、0、Decimal `1.10`/`-2.50`、Double。全部用例结果一致：

```
EQUIV| upper(s) = 'STRASSE'     where=Optional[(s IS NOT NULL) AND (upper(s) == 'STRASSE')]
EQUIV| lower(s) = 'i̇stanbul'    where=Optional[... (lower(s) == 'i̇stanbul')]
EQUIV| length(s) = 1           where=Optional[... (char_length(s) == 1)]
EQUIV| contains(s, 'a%b')      where=Optional[... (s LIKE '%a\%b%' ESCAPE '\')]
EQUIV| contains(s, 'O\'B')     where=Optional[... (s LIKE '%O''B%' ESCAPE '\')]
EQUIV| contains(s, 'a\\b')     where=Optional[... (s LIKE '%a\\b%' ESCAPE '\')]
EQUIV| big % 2 = 1             where=Optional[... ((big % 2) == 1)]
EQUIV| big / 2 = 0             pushed=false（只有 Spark 附带的 big IS NOT NULL 被推下）
```

`Tests run: 4, Failures: 0, Errors: 0`，首跑 9.88 s、热跑 4.47 s。反例（`/`、`abs()`、不可简化的 `LIKE 'a%b%c'`）都在 pushed 侧验证为"仍留 Spark Filter"。

### 7.3 对 FTS 放大点的实际影响

同一 FTS 表上：

```
FTS| pushed   k=5           filter=(id IS NOT NULL) AND ((id % 2) == 1) ids=[19,17,15,13,11]
FTS| residual k=2147483647  ids=[19,18,17,16,15]   (AND _score > 0)
```

即：新增映射把原来残留的 `id % 2 = 1` 变成可下推，`hasResidualPredicates=false`，`buildNamespaceFtsScan` 就会把真实 LIMIT 作为 `k` 发给 `queryTable`。这只缩小 `k=Integer.MAX_VALUE` 的触发面，**不声称解决大 k 的内存问题**。

## 8. 对 B'（rowid 范围递归切分）的含义

已具备的前提（事实）：

1. `_rowid` 范围 + `prefilter=true` 的 top-k 精确（§4），且不依赖同分顺序；
2. `k` 可以按范围设小（§4 的 k=5）；用 `k+1` 探测"该范围是否超过 k 个命中"是安全的（U2 分数精确、U1 `min(k, N-offset)`）；
3. Spark 在 residual 与非 residual 形状下都保留全局 Sort/Limit（#3282 U3），分段结果的并集再交给 Spark 排序/limit 不会漏；
4. rowid 在 append 与部分索引下稳定（§6.1/6.2）。

启用前还缺（未验证 / 需要实现）：

1. **snapshot pin**：每个递归请求必须带同一个 `version`；`OPTIMIZE` 会重写 rowid（§6.3），并发 append 也可能让"新范围"在请求之间出现。连接器当前的 namespace FTS 请求只在用户显式 `TIMESTAMP AS OF`/`version` 时带 version，B' 需要主动 pin。
2. **稳定 rowid 的大 u64 字面量**：本轮只测了小值；rowid 超过 `Long.MAX_VALUE` 时 SQL 字面量必须用无符号十进制，且 Lance 的解析/比较是否接受需要单独验证（§6.4 只覆盖小值）。
3. **代价未知**：源码层面 `prefilter_source` 的 mask 是"对索引覆盖的所有 fragment 做一次 row-id filtered read"（`scanner.rs:8353` 起），fragment 集合由 FTS 查询覆盖决定，**不随 rowid 范围收窄**；因此每段请求仍要付 O(索引覆盖行数) 的 row-id 读取。这个推断没有在本轮测量，10M 上必须实测每段延迟/IO 后再定分段策略。
4. B' 的循环控制（何时停止切分、最大请求数、取消/重试）与配置默认值仍需实现与审查。

结论：**B' 具备"开始实现"的正确性前提，但不具备"现在默认启用"的工程条件**。建议按 #3262 §14 的顺序：A（本卡已完成）→ B' 原型（pin version + 有界请求 + 10M 成本测量）→ 再决定默认值。

## 9. 未确认 / 本地不可达

- 以上全部是本地 `DirectoryNamespace` + local Spark 的行为；集群 namespace/REST 实现、真实 10M 表上的 `_rowid` 过滤代价未验证。
- `prefilter_source` 不按 rowid 范围裁剪 fragment 是源码推断，未做 profile；不能作为"每段代价有界"的结论。
- 稳定 rowid 的大 u64 字面量（> `Long.MAX_VALUE`）未测。
- `LIKE` 谓词的端到端语义未测（Spark 3.5/4.0 不会产生该谓词）；只有 SQL 渲染的单元测试。
- 同分顺序不稳定的结论沿用 #3282（U1/U2），本轮未重复；B' 的正确性论证刻意不依赖任何 tie 顺序。

## 10. 测试与文件清单

- 新增/扩展测试：`FilterPushDownTest`（新增映射的 SQL 渲染 + 安全边界）、`BaseFilterPushDownEquivalenceTest`/`FilterPushDownEquivalenceTest`（差分 + FTS k 取证）、`BaseFtsU5ProbeTest`/`FtsU5ProbeTest`（U5 探针）、`TestPredicates`（新谓词形状）、`LanceScanBuilderTest`（原先把 `CONTAINS` 当"不支持"的 3 个用例改用仍然不可下推的除法谓词）。
- 回归：`FtsScoreColumnTest`（25 通过）、`FtsCatalogOnlyNamespaceTest`（5 通过）、`LanceScanBuilderTest`（42 通过）、`FilterPushDownTest`（25 通过）通过；`FtsAdvancedOptionsTest`（20）与 `FtsPhraseMatchPushdownTest`（11）沿用既有 `assumeTrue(false)` 全跳过，与本次改动无关。
- 红/绿证据：把 `FilterPushDown` 换回改动前的版本重跑新测试，`FilterPushDownTest` 出现 6 个失败（新映射未生效）。
- 运行命令：

```bash
./mvnw -s /tmp/dsh-mvn-settings.xml -q -pl lance-spark-3.5_2.12 -am -Dspotless.skip=true \
  -Dtest='FilterPushDownEquivalenceTest' -Dsurefire.failIfNoSpecifiedTests=false test
./mvnw -s /tmp/dsh-mvn-settings.xml -q -pl lance-spark-3.5_2.12 -am -Dspotless.skip=true \
  -Dtest='FtsScoreColumnTest,FtsAdvancedOptionsTest,FtsPhraseMatchPushdownTest,FtsCatalogOnlyNamespaceTest,LanceScanBuilderTest,FilterPushDownTest' \
  -Dsurefire.failIfNoSpecifiedTests=false test
```
