# 10M 文本 BM25（k=10）基准格口径

日期：2026-11-25 · 卡片 #3285 · 单位 ms · 本格是 10M 五组向量基准之后**可选的第六格**，默认关闭
状态：**harness 已接入并通过离线测试；尚未在集群跑过**（由 root 在 C5 与 feature 镜像上执行）。本文只定义口径，不含任何推测数字。

## 1. 为什么单独定义一格

10M 五组是向量检索协议（`VECTOR_SEARCH` 表函数、100 条查询向量、真值 top-2000、recall 可算）。文本 BM25 是另一条链路：没有查询向量、没有可算的 recall 真值，返回的是 `doc_uuid + _score`，历史数字也来自另一套入口与缓存口径。把它硬塞进向量协议的列里会制造“看起来可比、实际不可比”的数字，所以单独定义一格：同一个镜像、同一套 spark conf、同一套窗口/UID/取证生命周期，但查询集、SQL、样本身份与报告行单独冻结。

## 2. 查询集（冻结资产）

| 项目 | 值 |
|---|---|
| 资产文件 | `scripts/benchmark/text-queries-10m.jsonl`（20 行，`qid` 0..19 + `query`，文件顺序即 qid 顺序） |
| sha256 | `9d04605cc228cb9b7f9a9bc22a73ef9fd917daa0eb4d51bb808800ac660d3bc3` |
| 合同 | `scripts/benchmark/text-10m.json`（冻结 k、表/列、索引、模板、warmup/repeats、资产 sha） |
| 来源 | `shai:/home/wanganyang/runs/3233/10m/text/lance-sql/lance.jsonl` |
| 来源 sha256 | `01e51d73da360a3c0baa4b97a1b9407940295313d616ed1eb0884752dde63fc2` |
| 来源镜像 | `hub.i.basemind.com/wanganyang/sparkann-e2e:r3-20261124-3d6fbf938b@sha256:70bb4930d429acb8b057f07fce337e47c337dad1a3176ad255a76e7a0eb6ea52` |
| 提取/校验脚本 | `python3 scripts/benchmark/text_asset.py --source <lance.jsonl> --out …`；`--check` 复核资产与合同 |

提取规则：只取 `TEXTQUERY` 记录的 `query` 字段，按首次出现顺序去重，必须恰好 20 条；资产按 `qid` 升序逐行写入、行尾换行，哈希针对**文件字节**而不是 JSON 语义。`submit.prepare_matrix`、`BaselineBenchmark`（JVM 内再算一次）和 `report.py`（读 run 目录里的副本）三处都校验同一个 sha256，任何一处不匹配即拒绝，不允许“换一批 query 也照跑”。

## 3. SQL 模板（历史原样，不走 SEARCH 表函数）

```sql
SELECT doc_uuid, `_score` FROM lance.smoke.mm_fixed
WHERE lance_match(content, '<query>') ORDER BY `_score` DESC LIMIT 10
```

- `{query}` 是模板里唯一占位符；单引号按 SQL 规则写成 `''`。
- 表 `lance.smoke.mm_fixed`，列 `content`，索引 `fmt_fts_content`（48 段，2026-11-24 建，本卡不重建、不改索引）。
- 返回契约：`doc_uuid`（非空、10 行内不重复）+ `_score`（有限值，且必须非递增，证明 `ORDER BY` 生效）。
- 历史记录的 20 条 measured SQL 与本模板逐字符一致（已核对 20/20），所以这不是“新造一条 SQL 再和旧数字比”。

## 4. 测量协议

| 维度 | 文本格 |
|---|---|
| 身份 | `kind="text"`、`k=10`、`qid` 0..19；每个 qid 先 1 次 warmup（`repeat=-1`）再 3 次 measured（`repeat` 0/1/2） |
| 样本数 | 80 条记录/格（20 warmup + 60 measured）；统计只用 60 条 measured |
| 计时 | `spark.sql(...)` 到 `collectAsList()` 完成，含分析/规划/调度；校验与 JSON 落盘不计入 |
| 缓存 | warm-session：warmup 之后才采样。FTS 索引缓存命中/驻留**没有** SQL 指标可证，`cache_evidence.residency_verified=false`，`verdict.cache_residency="unknown"`；不得声称索引常驻 |
| 资源 | 与五格同一合同：12×(4c,30g+12g)、driver 4c/12g+2g、task.cpus=1、index cache 6 GiB、metadata 1 GiB、AQE true、shuffle 48 |
| 入口 | `spark-submit --class BaselineBenchmark`，由 benchmark.py 的 `--text-cell` 追加在 batch-k2000 之后 |
| 窗口 | 与五格同一判定：样本前后轮询证据、`api_ok` 全真、BASELINE_DONE 标记；外来 benchmark 只记录不中止；不停止/删除任何他人作业 |
| 生命周期 | `submit.prepare_matrix` + `submit.run_one_cell`：唯一 pod 名、UID ledger、活动快照、driver 日志、event log、samples 拉取、`text-k10/verdict.json` |

`matrix-verdict.json` 在开启文本格时记 `cells=6`、`samples=156`（96 向量 + 60 文本），并附 `text_cell=true`、`text_queries_sha256`；不开启时字段与旧产物一致（`cells=5`、`samples=96`）。

## 5. 运行方式与前置条件

前置条件：

1. 表 `lance.smoke.mm_fixed` 的 FTS 索引 `fmt_fts_content` 已存在（48 段，本卡不重建）。
2. `scripts/benchmark/text-queries-10m.jsonl` 与 `text-10m.json` 的字节是冻结版本（sha256 见 §2）。
3. 已通过 10M 真值校验的 `queries`/`truth` 资产、洁净窗口与 `--stopped-evidence` 仍是五格的前置条件；文本格不降低任何一条。
4. 镜像 digest、资源、catalog（`impl=dir`、`root=s3://step5p0-data/.../lance-dir`、`LanceSparkSessionExtensions`）与五格同一份 `baseline-10m.json`。

命令（在 shai 的 runner 目录，或本地有 kubectl/docker/HDFS 的环境）：

```bash
# 只准备、只打印计划（零 subprocess；会打印文本格的 spark-submit 与 SQL 示例）
python3 scripts/benchmark/benchmark.py --queries <queries.jsonl> --truth <truth.json> \
  --output-root <run-root> --text-cell

# 正式执行：五格 + 文本格
python3 scripts/benchmark/benchmark.py --run --approved \
  --queries <queries.jsonl> --truth <truth.json> --output-root <run-root> \
  --stopped-evidence <verified-stopped.json> --text-cell

# feature 镜像：由 feature-run.py 转发 --text-cell（工具目录同步内含文本资产并校验哈希）
python3 scripts/benchmark/feature-run.py --run --approved --text-cell ...（其余参数同五格）
```

## 6. 与历史 BM25 报告（#3232 / R3 表）的口径差异

历史数字来自 iceberg-tools 的 3233 报告与本仓库累计表（`10m-comparison-cumulative.md` 表 2）：iceberg-index 参照 704（1569，表函数）/ 645（3632，SQL 改写）/ 1532（4780，CALL），lance 臂 4,796（11291）与另一窗口的 3,040。**这些数字不能与本格直接对比**，原因逐项如下：

| 维度 | 历史 lance 臂（#3232/R3） | 本格（#3285） | 影响 |
|---|---|---|---|
| 缓存 | 来源 `TEXTMETA` 明写 `cache="off"` | warm-session（warmup 后采样；FTS 缓存无取证） | 历史是冷态窗口，本格是暖态；方向不固定，不能当同一口径 |
| 样本 | 历史 JSONL 只记录 1 条 warmup（首个 query），20 条 query × 3 measured | 每个 qid 各有自己的 warmup，80 条记录/格 | 本格证据自洽；历史无法证明每个 query 都预热过 |
| 窗口 | 与 iceberg 竞品交错的历史窗口，另有 3,040 来自另一窗口 | 独立 run、同一窗口判定；不交错 | 两次历史数字本身不同窗口，不可拼成一条曲线 |
| 入口 | lance 臂 SQL 谓词；iceberg 704 是表函数、645 是 SQL 改写、1532 是 CALL | lance `lance_match` 谓词（历史 lance 臂同形状） | 与 iceberg 三个数字都不同入口，先把“谁的入口”说清楚再谈倍数 |
| 返回 | lance 回 `doc_uuid+_score` 10 行；iceberg 表函数/CALL 的行契约未在本仓库冻结 | 同上，且校验 10 行、唯一、score 非递增 | 只有 lance 谓词这条链路和本格同形状 |
| 资源 | 12×(4c,30g+12g)、driver 4c/12g，与 C5 相同 | 同（同一 spark conf 组装） | 资源对齐；但缓存/窗口/样本不对齐 |
| 索引 | 历史 `fmt_fts_content`（本表 2026-11-24 建） | 同一索引，不重建 | 索引对齐，可排除索引差异 |

结论：本格的文本数字只能与**同一版本本 harness** 的另一个 run（例如 C5 vs feature 镜像，或 #3263 起 FTS 链的 feature）比较；与 #3232/R3 表、与本仓库累计表表 2 的历史列只能做量级参照，不给倍数。历史数字里 4,796 与 3,040 的差异本身来自不同窗口与缓存开关，连“历史内部”都不构成同一口径。

## 7. 报告渲染

`report.py` 已支持文本格：

- 标准表新增一行：场景「文本 BM25」、`k=10`、单元格 `p50（p95）`（20 个 qid 的 measured 中位数的中位数；p95 对 60 条 measured 线性插值）。
- 文本行排在向量五格之后（`KIND_ORDER` 文本列最后），累计表可把它作为新增一列；比值同样按 p50 计算。
- 完整性表：文本格 recall 列显示 `n/a（文本无真值）`，不把“没有 recall”误报成缺样本。
- 每个 run 目录由 `prepare_matrix` 复制 `text-contract.json` + `text-queries.jsonl`；`report.py` 逐条把 samples 里的 SQL 与冻结模板 + 冻结 query 比对，不一致进「缺数与口径说明」；缺副本或 sha 不一致则标“文本格证据不完整”，数字仍展示但不被当作已核对。
- 文本格**没有**接入 `pair-run.py` 的交错窗口：跨臂文本比较要两臂各自成对重跑并检查窗口，不能拿两次独立 run 的文本格直接相减。

## 8. 已知限制（必须与表同读）

1. 无 recall 真值：BM25 的“正确性”在本格只校验返回形状与排序，不校验召回；需要质量结论时另建判定集，不能从本格推。
2. FTS 索引缓存无取证：`residency_verified=false`，只能说“同 session 预热后”，不能说“索引常驻”。
3. 文本格单独追加、不交错：与 `pair-run.py` 的成对设计不同，污染窗口的处理方式与五格一致（记录、不作废、不停止他人作业）。
4. 历史 4,796/3,040 不进入本格的任何比值或结论。

## 9. 取证字段（run 目录）

- `manifest.json`：`text_cell=true`、`text_contract_sha256`、`text_queries_sha256`、`text_cell_detail{k,queries,warmup_passes,repeats,sql_template}`。
- `text-k10/samples.jsonl`：每条 `kind=text, k=10, qid, warmup, repeat, started_ms, ms, rows, complete, sql, query, results[{doc_uuid,_score}], plan, cache_evidence, timing`。
- `text-k10/verdict.json`：与五格同构（`window_usable`、`exclusive_window_valid`、`contamination`、`foreign_observed`、`done_marker_present`、`eventlog_present`），并带 `pod/uid/app/state`。
- run 根目录：`text-contract.json`、`text-queries.jsonl`（冻结身份副本，报告据此复核）。
