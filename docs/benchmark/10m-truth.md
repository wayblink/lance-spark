# 10M 完整精确真值生成（#3271）

本工具只生成真值，不报告 ANN 性能。冻结业务镜像 `b102cc7 / Lance14.0.3`、数据 version56、10,784,907行、586fragments、100条2560维query；正式输出每query完整top2000。查询原parquet SHA256为 `dd29359aecf3fdef8de7ba3dbb21743257851eb72c99514b459c7aa15d4c7f63`，按qid排序的 `<int64 qid, int32 dim, float32 little-endian coords>` SHA256为 `e60ccd2f1e2193e8d4c0fc25a4cdae7ac2ebe75621d7080a1cad65a888b4d95a`。输入合同由同目录的基线工具维护，本卡不复制或修改它。

## 算法与正确性

`TruthGenerator` 通过 `Utils.openDatasetBuilder` 打开固定version56，读取完整fragment清单和行数。每fragment一个Spark task，Dataset打开一次，一次native `setKeys` exact bypass scan回答本query块：`useIndex=false / metric=L2 / prefilter=true / refine absent / queryParallelism=1`。仅取native每query局部top2000的 `doc_uuid + _rowid + _distance`，不输出原向量、正文或整行。NULL向量/已删除行交给固定snapshot的原生扫描排除；保留query自身匹配，不删除0距离结果。

每query以 `(float32 distance, signed int64 rowid)` 排序；本表rowid小于2^63，拒负rowid。rowid是实体身份；doc_uuid允许重复，不按doc_uuid去重。doc_uuid与rowid从同一Arrow记录读取，不依赖takeRows返回顺序猜映射。

每个task只保留 `block_size × 2000` 的bounded heaps及fragment BitSet。`treeReduce(depth=3)`逐层合并同样有界的Block，检查coverage交集，拒重复分片；不把586片全部候选collect到driver，不做全候选全局sort。最后每query必须恰2000，scan count/BitSet都必须是完整586且包含每个预期fragment。query块默认10，可20；每task堆最多20,000/40,000 hits。精确打分仍需扫描全向量，不能把有界输出误解为没有全扫描成本。

## 编译与正式运行

由主代理复用基线提交器的唯一run、UID ledger、eventlog、超时取消通道；本工具不另写kubectl轮询，不删除任何表/索引/pod。用户B阶段已批准，实际submit仍由主代理审查后统一执行。建议同一资源12×4core、30g heap+12g overhead，driver4core/12g；预计30–90分钟，硬停120分钟，真值不与正式C计时并行。

镜像内编译（仅编译独立probe，不构建产品镜像）：

```bash
javac -proc:none -cp '/opt/spark/jars/*' -d build/classes \
  scripts/benchmark/TruthGenerator.java
jar cf build/TruthGenerator.jar -C build/classes .
```

通过统一安全提交器设置同基线的镜像digest、storage env、Lance缓存6GiB+1GiB、HDFS eventlog，main class改为 `TruthGenerator`，application参数：

```text
<baseline-10m.json的HDFS或可达URI> <本轮唯一HDFS输出目录> 10
```

例如主代理核准的 `spark-submit --class TruthGenerator ... TruthGenerator.jar CONTRACT_URI OUTPUT_URI 10`。参数中不得放访问密钥。需要继续同一次未完成运行时，显式末尾加 `--resume`，block size/manifest必须完全相同。

## 持久化与恢复

新run目录必须不存在；创建 `run-manifest.json` 冻结metadata、block_size、完整fragment清单及hash。每块依次create-only写 `block-000-009.json` 和同名 `.sha256`；写入后重读核验checksum、身份、全586coverage、query范围、每query2000条、rowid唯一、distance/rowid顺序。只有全部块复核通过，才写 `generation.complete.json`，日志最后输出 `TRUTH3271 done {ok:true,...}`。异常不写complete。

`--resume`只跳过checksum和内容都正确的完整块。不允许以旧query/snapshot续写、不覆盖旧块。若块内容存在但checksum缺失（中断在两次create之间），工具拒绝继续；保留证据，由主代理明确决定恢复该块或用新run，不自动删除/修补不明数据。缺块不会被最终拼装误标成功。

下载本轮输出到本地后离线完整校验并拼装：

```bash
python3 scripts/benchmark/truth.py validate --contract scripts/benchmark/baseline-10m.json \
  --directory <本轮下载目录> --block-size 10
python3 scripts/benchmark/truth.py assemble --contract scripts/benchmark/baseline-10m.json \
  --directory <本轮下载目录> --block-size 10 --output <新truth.json>
```

拼装输出的 `metadata` 与基线 `truth_identity`精确一致，query为 `{qid,ids,rowids,distances}`；运行信息单独放顶层provenance，不污染合同字典。最终truth文件及`.complete.json`均create-only；complete内有truth SHA256。基线runner还须执行自己的validate-assets；不能仅凭done日志认定资产完整。

## 本轮验证状态

离线Python测试先因缺truth.py失败，再7/7绿（4workers）；覆盖top-k集中一片、同距、重复docid、boundedmerge、hash、缺coverage/缺query/错块checksum。Java实际TopK/Block另有5项并发检查：集中赢家、同距、随机有界合并对全sort、coverage拒重与序列化。Java工具只本地JDK17编译通过，没有本轮提交Spark，也没有生成正式100×2000真值。两fragment/三query的A4 pilot只证明小N路径，不能替代完整586fragment验证。

## 正式 B 执行证据

2026-11-25 B已获用户批准并由root授权执行。run `b3271-truth-20261125043442-d5358f`、driver UID `6c065647-f113-43c7-ae8d-d69221fc5432`，固定镜像digest `374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb`，完整hostname `cpu-1122.host.platform.shaipower.com`；只读健康检查Ready=True、taints为空、Disk/Memory/PIDPressure=False。资源12×4c/30g+12g、driver4c/12g，native cache6GiB+1GiB。生成与下载拼装总墙钟407.148秒；这是资产准备，不是ANN基准成绩。

实际执行Java源固定为提交 `f825711`，SHA256 `03cc81c10be92635edb4652299cf687cbaa361180cf955e3a4c73bb6aa11a2ab`；实际launch脚本当时SHA256 `83848756148de5c465fa4905b1ec3b34adfe3c7811d5d184311995fa2697ba72`，通过CLI显式传完整节点名。后续提交的 `truth_submit.py` 增加默认完整节点名、启动umask077、压力/taint门禁、pod证据字段脱敏、只watch非敏感列，以及采证失败仍写verdict；没有热改运行Java。已运行目录随后chmod700，敏感证据600并脱敏，不声称旧launch已包含后续所有加固。

10块全部完成，每块coverage586；全100query×top2000=200,000rowid实体，原parquet hash、canonicalfloat32 hash、每query唯一rowid及distance/rowid顺序通过 `baseline.validate_assets`。shai本地资产：

- `/home/wanganyang/runs/3271-truth-b1/truth.json`：11,770,072B，SHA256 `66465c296d4771c3c7bcf5e191914f33dd8708229e9fc96d2f40ed0034fe7c02`。
- `/home/wanganyang/runs/3271-truth-b1/queries-f32.jsonl`：文件SHA256 `9e90bc2fdb3b3d5796e718f1d8f5aae156e472fea13eea2668e48e45f6178551`，canonicalfloat32仍为上述 `e60ccd2f…`。
- `/home/wanganyang/runs/3271-truth-b1/truth/run-manifest.json`：SHA256 `a2e8b9f7c744bb3f93e1935ce5232d1c068c9d0925dd3a019b4bf79ae760dc34`；最终验证记录 `validated-assets.json`。

HDFS原块与eventlog保留于 `/user/wanganyang/sparkann/bench/lance-dev-3255/b3271-truth-20261125043442-d5358f/`；无表/索引改动。B与post-validation两后台job均已collect退出；本agent没有启动C。
