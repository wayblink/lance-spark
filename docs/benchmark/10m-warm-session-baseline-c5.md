# 检索基准对照报告：c5（仅对照组；未指定本方）

- 生成时间（UTC）：2026-11-25T07:43:50Z；工具：`scripts/benchmark/report.py` sha256 `c322b25b4035a2269ef3eb80e34d1ac61ba1c30ce83878e07c94d1e527a2738d`。
- 对照组 `c5` = `/home/wanganyang/runs/3255-matrix-c5`；本方未指定。
- 单位 ms；单元格 `p50（p95）`，批量格 `总耗时（每查询耗时）`；`*` = 该格 measured 不完整，`n/a` = 无数据或 harness 判定无效（不以 0 代替）。
- 本报告只读后处理既有产物，不提交作业、不改测量语义。

| 场景 | k | 对照组数值 | 本方数值 | 比值 |
| --- | --- | --- | --- | --- |
| 向量单查询 | 10 | 2900.2（7386.6） | n/a（未指定本方） | n/a |
| 向量单查询 | 100 | 4555.6（7935.4） | n/a（未指定本方） | n/a |
| 向量单查询 | 2000 | 22807.9（26486.5） | n/a（未指定本方） | n/a |
| 批量 100 查询 | 100 | 45082.5（451.2） | n/a（未指定本方） | n/a |
| 批量 100 查询 | 2000 | 66726.7（726.4） | n/a（未指定本方） | n/a |

## 数据规模与索引参数

| 项目 | 对照组 | 本方 | 对齐/偏差 |
| --- | --- | --- | --- |
| 行数 | 10784907 | n/a | —（仅一侧有数据） |
| fragments | 586 | n/a | —（仅一侧有数据） |
| 版本 | 56 | n/a | —（仅一侧有数据） |
| 表名 | lance.smoke.mm_fixed | n/a | —（仅一侧有数据） |
| dataset_uri | s3://step5p0-data/wanganyang/iceberg-test/full/lance-dir/dfdfd9b0_smoke$mm_fixed | n/a | —（仅一侧有数据） |
| 向量列 | qwen_3_4b_embedding | n/a | —（仅一侧有数据） |
| id 列 | doc_uuid | n/a | —（仅一侧有数据） |
| 维度 | 2560 | n/a | —（仅一侧有数据） |
| 距离口径 | l2 | n/a | —（仅一侧有数据） |
| 索引名 | r3-ivf-4b | n/a | —（仅一侧有数据） |
| 索引段数 | 32 | n/a | —（仅一侧有数据） |
| nprobes | 8 | n/a | —（仅一侧有数据） |
| refine_factor | absent | n/a | —（仅一侧有数据） |

- 对照组 合同：baseline-10m.json（来源：仓库 scripts/benchmark/baseline-10m.json；sha256 与 manifest.contract_sha256 一致）
- 对照组 样本内嵌 SQL 与合同索引参数：96/96 条一致

## 资源、镜像与 harness

合同要求（baseline-10m.json）：12 × (4c, 30g+12g)，driver 4c/12g+2g，task.cpus=1，index_cache 6.0 GiB，metadata 1.0 GiB，AQE true，shuffle 48。

| 项目 | 对照组 | 本方 | 对齐/偏差方向 |
| --- | --- | --- | --- |
| executor 数 | 12 | n/a | —（仅一侧有数据） |
| executor cores | 4 | n/a | —（仅一侧有数据） |
| executor heap | 30g | n/a | —（仅一侧有数据） |
| executor overhead | 12g | n/a | —（仅一侧有数据） |
| driver cores | 4 | n/a | —（仅一侧有数据） |
| driver heap | 12g | n/a | —（仅一侧有数据） |
| driver overhead | 2g | n/a | —（仅一侧有数据） |
| task.cpus | 1 | n/a | —（仅一侧有数据） |
| AQE | true | n/a | —（仅一侧有数据） |
| shuffle partitions | 48 | n/a | —（仅一侧有数据） |
| index cache 预算 | 6.0 GiB（capacity=6442450944） | n/a | —（仅一侧有数据） |
| metadata cache 预算 | 1.0 GiB（capacity=1073741824） | n/a | —（仅一侧有数据） |
| distributed search | true | n/a | —（仅一侧有数据） |
| 镜像 | hub.i.basemind.com/wanganyang/sparkann-e2e:r3-20261125-lsbatch-b102cc7@sha256:374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb | n/a | —（仅一侧有数据） |

- 对照组 实际 spark-conf 与合同逐项一致（12 项）
- 对照组 镜像 digest：sha256:374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb；合同 image_digest 匹配：是
- 对照组 harness commit：未记录（run 产物无 harness_commit/git_commit 字段，本工具不推测）

## 窗口口径

- 只排除「可识别的、新增的外来 Spark pod」（submit.activity() 归为 benchmark/unknown 的活 driver/executor）；Kyuubi/ThriftServer/Spark Connect 等常驻 background 允许作为共享背景，但每格必须在样本前后都有轮询证据。
- 判定依据：每格 verdict.exclusive_window_valid，以及 activity.jsonl 的轮询记录。
- 对照组 `c5`：窗口有效 5/5 格（有 verdict 的格）；activity 轮询 102 次，API 失败 0 次，含外来 pod 的轮询 0 次（其中 unknown 0 次）。

## 缓存口径

- 采用 warm-session：每个 identity 先执行 warmup（repeat=-1），measured 只取预热后的 3 个 repeat；本表不包含任何冷启动数字。
- native index-cache residency **unverified**：只有 samples 里 cache_evidence.residency_verified=true 才算证据。
- 对照组 `c5`：residency_verified=true 的 measured 样本 0/96；verdict.cache_residency=unknown。因此只能声称「同 session 预热后」，不能声称索引常驻内存。

## 离群样本（不剔除，仍计入 p50/p95）

| 场景 | k | 运行 | qid | repeat | ms | 占该格 measured 总耗时 | ×该 query 中位数 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 向量单查询 | 10 | 对照组（c5） | 8 | 1 | 126881.4 | 55.5% | 20.8× |

这些样本不是测量误差诊断结论：p50 用中位数，不受单点影响；p95 是否被抬高取决于该样本的排序位置（n=30 时只插值到第 27.55 位，最前面 2–3 个样本才进 p95），必须对着 p95 与离群占比一起看，不能假定「p95 已包含它」。

## 样本完整性与召回

| 场景 | k | 运行 | measured | warmup | complete=false | recall_min | 窗口有效 | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 向量单查询 | 10 | 对照组 | 30 | 10 | 0 | 0.80 | true | — |
| 向量单查询 | 100 | 对照组 | 30 | 10 | 0 | 0.88 | true | — |
| 向量单查询 | 2000 | 对照组 | 30 | 10 | 0 | 0.81 | true | — |
| 批量 100 查询 | 100 | 对照组 | 3 | 1 | 0 | 0.65 | true | — |
| 批量 100 查询 | 2000 | 对照组 | 3 | 1 | 0 | 0.53 | true | — |

## 缺数与口径说明

无（所有格 measured 完整、recall 齐全、verdict 有效）。

口径提醒：
- `n/a` 表示无数据或 harness 判定无效，绝不以 0 代替；`*` 表示该格 measured 不完整。
- 比值按单元格 p50 计算（批量格用整批总耗时 p50；两侧 N 相同时与每查询比值等价）。
- 本表是只读后处理，不重新测量；任何重新计算都必须换新 run 目录，而不是改本脚本的输入。

