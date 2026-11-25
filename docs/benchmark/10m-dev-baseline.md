# 10M dev 暖查询基线合同与 A 阶段探针

卡片 #3255；业务基线 `b102cc7`，dev 文档提交 `ab7bce6`。这里只是基准工具和固定参数，不包含产品实现改动。**正式 100×top2000 truth 与五组测量尚未执行。**

## 可复用资产

- 业务镜像已存在：`hub.i.basemind.com/wanganyang/sparkann-e2e:r3-20261125-lsbatch-b102cc7@sha256:374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb`。Lance14.0.3 bundle SHA256 `3ee4c4fb82130d4cf0506e36af9f6474c1105689519276d1a4c39981f4136ade`；dev 后面的 feature 不得混入原始 baseline 镜像。
- 表 `lance.smoke.mm_fixed`，URI `s3://step5p0-data/wanganyang/iceberg-test/full/lance-dir/dfdfd9b0_smoke$mm_fixed`，已只读确认 version56、10,784,907 行、586 fragments，向量 `qwen_3_4b_embedding` 为2560维float32；id字段doc_uuid/uniq_id都是string。
- 命名索引 `r3-ivf-4b`，32段IVF_RQ，段生成版本54。排序UUID用换行连接（无末尾换行）的SHA256 `f0a200b9f674f1af55b3a875770dc7b9d4961de7e8e4d374f065d12d92e088b9`；完整UUID冻结在 [baseline-10m.json](../../scripts/benchmark/baseline-10m.json)。不要执行默认包含质心的大索引statistics；设置 `LANCE_INCLUDE_VECTOR_CENTROIDS=false`。
- 固定100query资产：HDFS `…/user/wanganyang/sparkann/bench/format/topk-formal/queries100.parquet/part-00000-57b65963-29d7-49c7-a04a-44cfdd32dd66-c000.snappy.parquet`，401381B；SHA256 `dd29359aecf3fdef8de7ba3dbb21743257851eb72c99514b459c7aa15d4c7f63`；已读元数据确认100行、qid int64唯一0..99、src_id string、vector double长度全2560。正式truth应把向量cast到float32，并记录canonical float32 payload hash与原parquet hash。
- shai 接入 `ssh shai`；KUBECONFIG=`$HOME/workspace/k8s-config/clusterA.yaml`，HADOOP_HOME=`/opt/hadoop-3.3.1`，SPARK_HOME=`/opt/spark`，JAVA_HOME=`/usr/lib/jvm/java-17-openjdk-amd64`。对象存储凭证从平台spark-defaults读取，不写入params/git/结果。

## 必须新补，不能直接复用旧 R3

1. 完整10M×100query×top2000 truth。旧 `oss32-r3-10m-20261124/truth.jsonl` 仅11978B，深度10，不是完整高k真值；旧100M真值不能替代10M。
2. 单查/真批量统一真实collect结果合同。旧TopKProbe pure模式是100次单查循环且SELECT count(*)；旧LanceBatchProbe只SELECT query_id，缺命名索引、搜索列和参数，smoke曾因找不到默认vector列失败。
3. 主五格必须外层窄SELECT、**不传native columns**以捕捉projection问题；显式columns只作单独诊断，不混主表。为消除doc_uuid可能重复的歧义，正式测量启动前统一要求with_row_id=true、外层包括_rowid：

```sql
SELECT doc_uuid, _rowid, _distance
FROM VECTOR_SEARCH(table => 'lance.smoke.mm_fixed', query_vector => array(...),
  vector_column => 'qwen_3_4b_embedding', index => 'r3-ivf-4b', version => 56,
  k => 10, nprobes => 8, distance_type => 'l2', with_row_id => true);

SELECT query_id, doc_uuid, _rowid, _distance
FROM VECTOR_SEARCH(table => 'lance.smoke.mm_fixed', query_table => '_baseline_q100',
  query_id_column => 'qid', query_vector_column => 'vector',
  vector_column => 'qwen_3_4b_embedding', index => 'r3-ivf-4b', version => 56,
  k => 100, nprobes => 8, distance_type => 'l2', with_row_id => true);
```

refine参数固定省略（no-refine）；**不传0，不沿用旧R3把0翻译为1**。索引结果距离是量化估计，正式recall与exact truth比较行标识，不要求逐条距离等于exact；single/batch同参数可验证结果一致。

4. runner生命周期与窗口。#3244等待NotFound挂到12h、#3245终态pod和executor漏判在工具主仓仍未修；远端grid只有临时终态过滤，不能当正式修复。新run必须唯一、create-only，不先删同名pod；UID ledger隔离清理，API故障≠POD_GONE，超时有界采证。正式测量按活跃driver/executor身份/UID/app选择，不按名字前缀猜；忽略终态，不能删除其他作业来腾窗口。

## 冻结资源与计时

12 executors×4 cores×30g heap+12g overhead；driver4c/12g+2g，task.cpus1，AQE=true，shuffle partitions48，dynamic allocation=false。native index cache6GiB、metadata1GiB，不抄iceberg的22GiB预算（native计入容器非堆，可能OOM）。同一feature重跑不能悄改nprobe/refine、并行度、索引或快照。

五格：single k10/100/2000，固定qid0..9各3 repeats；true batch100×k100/2000，各整批warmup1+测量3。每个single query先warmup，再自己的samples。计时从 `spark.sql` 前到 `collectAsList` 返回，包含batch的query_table分析期读取；不能只计执行阶段漏掉批量入口成本。记录started_ms、每query id/rowid/dist、plan、任务metrics和eventlog。

原b102cc7已有SQL IO/parts/open/create/load指标，但未把ScanStats index cache hits/misses映射到Spark。**“预热后测量”不等于已证全部驻留**：基线cache_residency=unknown；driver SDK probe的cache数字不能冒充executor SQL session；不伪造entries/coverage。后续metrics-only feature用同合同重跑后可补真实cache证据。

## 离线验证与当前工具

```bash
python3 scripts/benchmark/run-tests.py
python3 scripts/benchmark/baseline.py dry-run
python3 scripts/benchmark/baseline.py validate-assets --queries queries-f32.jsonl --truth truth-top2000.json
```

stdlib并发workers4；先红后绿，10个关键不变量覆盖：固定五格/资源/refine、完整truth、真batch结果数、每query预热与3repeats、未知cache不冒充驻留、float32hash、终态/UID窗口、POD_GONE/API区别和白名单清理。正式Spark五格执行器与truth生成器尚需后续review，不把dry-run称作基线完成。

[BaselinePilot.java](../../scripts/benchmark/BaselinePilot.java)仅A阶段correctness：3query named-index single/batch k10对齐，显式columns诊断；对同version的2个fragments做native batch bypass exact与逐fragment local top10归并一致、takeRows rowid→doc_uuid顺序映射。它不是完整truth，不产正式性能。

[pilot.py](../../scripts/benchmark/pilot.py)远端需要显式 `--approved`，compile300s/submit180s/整run1800s；唯一pod、ledger、事件、最后状态、日志脱敏、UID fenced deadline cancel。它可指定driver node仅用于correctness，必须报告偏差，不能把更换节点视为正式基准资源对齐。

## A 阶段已发生的基础设施结果

- A1：`shai:~/runs/3255-pilot-a1`；pod `b3255-pilot-20261125033744-d500c8`，UID `fd6586c8-d637-4642-9bc1-32b71857858f`，cpu-1066，56s内ephemeral-storage驱逐，0query/0marker。JAR编译成功。
- A2：`shai:~/runs/3255-pilot-a2`；pod `b3255-pilot-20261125034009-43eefd`，UID `da7eda9b-fbfb-4659-b587-30589a632f89`，cpu-1001，65s内同类驱逐，0query/0marker。
- A3：`shai:~/runs/3255-pilot-a3`；pod `b3255-pilot-20261125034359-6a505a`，UID `084d3888-348a-402c-868a-f0e85a74af30`；指定cpu-0149但有sriov特殊taint，Pending/Unschedulable，0query；root明确要求UID取消后记录 `UNSCHEDULABLE_INFRA`，保留describe/events/ledger。
- A4：用户明确额外授权，指定已只读核Ready/DiskPressureFalse/无taint的cpu-1122；输出 `shai:~/runs/3255-pilot-a4`。pod `b3255-pilot-20261125035654-2df8ef`，UID `5792cfcf-bcef-42ca-a7d5-bc9d0a9e1925`，140.1s后Succeeded，进程exit0，9条BASEPILOT标记：3query单查与真batch30行逐query完全一致；2fragment exact grouped/local merge/takeRows映射全部PASS。收取Spark eventlog1,241,660B，AppID `spark-b322f4dab2fd4cf39c1f60945e54b161`，executor-added事件12条（4core/个），经历executor更换，不能当健康独占性能窗口。driver SDK flat诊断cache0不代表SQL executor缓存。无正式truth/五组、无A5自动重试、无B/C隐式授权。

## 向用户请示的预算（不是实测承诺）

| 阶段 | 乐观 | 通常/上限 | 门禁/风险 |
|---|---|---|---|
| 复用现镜像、编译JAR+小N pilot | 5–10min | 10–20min，硬停30min | 目前node ephemeral驱逐；query开始前0marker不算产品失败 |
| 100×top2000完整exact truth | 15–30min | 30–90min；慢OSS/分块可1–2h，硬停120min | 先小N exact/group/local merge/rowid门禁；586fragment、float32同queryhash，不能1B行全量sort |
| single三格（10qid×3） | 15–25min | 25–45min | 同query warm-up、快照和索引、真实取行 |
| batch100×k100（1warm+3） | 8–15min | 15–30min | 已有setKeys，不循环100SQL |
| batch100×k2000（1warm+3） | 20–40min | 40–90min，整格硬停120min | 原宽行为可能spill/OOM/挂起；失败留基线，不改columns/chunk救成功 |
| 每feature新bundle+薄镜像 | 10–20min | 通常15–35min | 若只换jar层；不重建Rust，不动原baseline镜像 |

正式truth建议按native batch bypass exact分块10–20query，每fragment只保留local2000，再有界合并；truth专用窄输出/分块不混主基准。基线所有五格同主SQL/宽native行为，不能为了让batchk2000完成而偷偷分块或裁列。正式测量还必须等待独占窗口；namespace隔离并不隔离CPU/IO，绑定driver健康节点也不保证executor/整窗口干净。11:16只读观察共享集群仍有约52个benchmark相关非终态pods，因此当前只能correctness，不可出正式延迟结论。
