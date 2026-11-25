# 10M 对照参照臂：iceberg-index（lance-iceberg）暖态

日期：2026-11-25 · 关联卡片 #3254 / #3233 · 用途：所有 10M 性能表固定携带的第三方参照列。

## 1. 为什么单列一份参照

`docs/benchmark/10m-warm-session-baseline-c5.md` 是**同 harness 同协议**的本方基线；本文是**另一套实现**的第三方参照。两者不可混算倍数，原因见第 3 节。

## 2. 参照数字（10M，2026-11-24，产品 `index@3d6fbf938b`，镜像 `r3-20261124-3d6fbf938b@sha256:70bb4930d429acb8b057f07fce337e47c337dad1a3176ad255a76e7a0eb6ea52`）

| 场景 | k | iceberg-index（暖态） | 备注 |
|---|---|---|---|
| 向量单查询 | 10 | 2088.4 ms（p95 4390.9） | 返回 k 行；样本 20/20 |
| 向量单查询 | 100 | 7765.5 ms（p95 10476.6） | 样本 20/20 |
| 向量单查询 | 2000 | 68154.5 ms（p95 70156.1） | 样本 5/5，p95≈max |
| 批量 100 查询 | 100 | 31262 ms（312.6 ms/查询） | positions 口径，**不回表** |
| 批量 100 查询 | 2000 | 36968 ms（369.7 ms/查询） | positions 口径，**不回表** |
| 文本 BM25 | 10 | 704 ms（p95 1569，表函数） | 同表另有 SQL 改写 645（3632）、CALL 1532（4780） |

数据 `lance.smoke.mm_fixed`（10,784,907 行 × 2560 维），索引 `r3-ivf-4b` 33 文件、IVF/RaBitQ 4bit、nlist≈583，nprobe=8、refine_factor=0、warm-up 1；资源 12×(4c,30g+12g)、driver 4c/12g、cache 22 GiB（PIN_ALL，`entries=33`、`bytes=23,313,172,336`、`coverage=1.0`）。recall（truth 前缀口径）k=10 0.95/0.97、k=100 0.98/0.99、k=2000 1.00/1.00。原始出处：`iceberg-tools-wt/3233/docs/r3-standard-table-2026-11-24.md`。

## 3. 与本次口径的对齐审计（必须与表同读）

| 维度 | iceberg-index | 本方（C5 / feature 镜像） | 偏差方向 |
|---|---|---|---|
| executor | 12 × 4c × 30g + 12g | 同 | 对齐 |
| driver | 4c / 12g | 同 | 对齐 |
| 数据/维度/版本 | 10,784,907 × 2560，version 56 | 同 | 对齐 |
| nprobe / metric / refine | 8 / l2 / 0 | 8 / l2 / absent | 对齐 |
| 索引结构 | 33 文件 IVF/RaBitQ 4bit，nlist≈583 | 32 段 IVF_RQ | **不对齐** |
| 召回 | k=10 0.95/0.97、k=100 0.98/0.99、k=2000 1.00 | 0.80 / 0.88 / 0.81 | **不对齐**，参照臂返回更准的结果 |
| native cache 预算 | 22 GiB | 6 GiB | **对 iceberg 有利** |
| 入口 | SQL 改写 `ORDER BY vec_distance LIMIT k` 的端到端应用时间 | `VECTOR_SEARCH` 表函数 | **不对齐** |
| 批量是否回表 | 否（positions） | 是（`query_id/doc_uuid/_rowid/_distance`） | **批量行不可比** |
| 样本数 | 20/20/5 | 30 measured + 10 warmup | 不对齐 |
| 测量日期/镜像 | 2026-11-24 | 2026-11-25 | 不同期 |

结论：**只作量级参照，不给倍数**。小 k 处 iceberg-index 更快、k=2000 处明显更慢、批量两行不成立；任何"我方比它快/慢 N×"的说法在召回与缓存预算不对齐前都不成立。要真对齐，需在 iceberg-tools 侧按 6 GiB 缓存、同召回口径、回表的配置重跑一次。

## 4. 表格固定格式

后续 10M 报告表统一为：`场景 | k | iceberg-index（参照） | 对照组（上一版） | 本方（当前 feature） | 比值（同 harness 配对）`。iceberg 列不参与比值计算，且每次都要带第 3 节的对齐审计或指向本文。
