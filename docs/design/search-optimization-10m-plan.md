# dev 检索优化实施与 10M 逐 feature 基准

日期：2026-11-25；伞卡 #3254。用户已批准建立子卡并开始优化，要求先建立10M基准，每个大feature完成后重跑五组。本计划承接 [dev分析](../analysis/lance-spark-search-optimization-analysis.md)，其中的预计倍数不是验收成绩。开发、测试、提交均在各卡独立 worktree，从 dev 创建并最终 rebase/FF 合回 dev；main 不用于本轮开发。

## 1. 五组固定查询合同

| 场景 | k | 查询集合/结果 |
|---|---:|---|
| 向量单查询 | 10 | 固定query集逐条执行；取回真实 id/distance |
| 向量单查询 | 100 | 同一固定query集；取回真实 id/distance |
| 向量单查询 | 2000 | 同一固定query集；取回真实 id/distance |
| 真批量100查询 | 100 | 一次VECTOR_SEARCH setKeys，返回query_id/id/distance，10,000行（数据量满足时） |
| 真批量100查询 | 2000 | 一次VECTOR_SEARCH setKeys，返回query_id/id/distance，200,000行（数据量满足时） |

单查询建议固定10条×3重复，批量100条建议整批3重复；每格先warm-up再采样，最终次数在preflight后冻结。五组不包括BM25；文本/hybrid各卡另加专项验证，不替换五组。标准表按 `场景 | k | 原始基线 | 本次feature | 比值`，single为p50（p95），batch为总时间（每查询时间）；同时附上一feature对照、样本数、recall、失败/超时与阶段数据，不混冷态或旧版本数字。

基线业务代码为 dev `b102cc7`（当前 dev `ab7bce6` 只增加分析文档），connector0.9.1-beta.1/Lance14.0.3/namespace0.12.0；运行时Spark/JDK、native/JAR及镜像digest必须按实物preflight记录，不凭pom猜镜像。候选镜像 `r3-20261125-lsbatch-b102cc7@sha256:374dddc63fc0de9b52bb604135594a38298c920be2950e9f1c87be72eb2ee0fb` 尚未证明成功smoke，只是可复用候选。

## 2. 基线前必须冻结的资产

由#3255核验并保存机器可读manifest：

- 10M表实际行数、URI、pinned version、fragment清单、schema/hash，查询全程只读，不重建/删除既有索引。
- 向量列 `qwen_3_4b_embedding`、维度2560；索引名/UUID/段数、各段IVF参数、量化位数、metric、覆盖；必须实际SHOW/日志确认named index命中，不依赖manifest顺序。
- 单查10条、批量100条向量资产及顺序/hash；每条完整top2000 truth及其snapshot、算法/metric、NULL/删除处理、确定性tie口径。旧10M仅10id的truth不得作为recall@100/2000。
- 资源每项写具体数字。候选profile：12 executors×4core、30GiB heap+12GiB overhead；driver4core/12GiB，dynamic allocation关闭、task CPUs明确；最终按可运行容器/预算核定。native index/metadata cache的bytes与每JVM合计独立核定，不能把iceberg heap cache预算照搬成Lance额外native内存。
- nprobes、refine（absent和0不能混义）、distance、fast_search、prefilter、AQE、shuffle partitions、WindowGroupLimit阈值、native query parallelism、IO/CPU线程配置全部冻结；后续只有对应feature可改变受控变量并记录。
- 冻结用户外层窄SELECT，native不显式给columns，测真实当前TVF行为以验证自动列裁剪收益；显式columns诊断另列，不混主表。不得用count/query_id-only/materialize-free路径代替真正id/distance collect。

## 3. 测量纪律与长任务

1. 正式计时必须独占测量窗口。只读观测既有driver/executor；不删除、不停止非本轮对象。观测到干扰时等待窗口或报告不可用，不硬出有效格。
2. correctness smoke与正式计时分离。先小查询确认镜像、维度列、named index、batch归属/行数，再跑完整矩阵。没有完整truth时先找资产，找不到才在说明耗时风险并获确认后生成。
3. 每格/每query都有deadline和阶段标记；超时采集driver/task/native证据，保留失败格。不靠无限轮询，不因挂起悄悄切换参数、columns或batch大小。
4. 记录Spark event log、logical/physical plans、task分布、dataset-open/scanner/batch-load、IO/parts/comparisons、SDK可用cache hits/misses、GC/native内存和样本时间戳。现有产品未转发的cache计数可用不改变产品的probe采证；无法观测的明确unknown。
5. warm-up之后才采样，确认bytes/命中行为；不把cache enabled直接当全驻留。只报暖态，不做cache=off正式指标。
6. job/pod/output使用本轮唯一id和创建ledger；finally只清理ledger内自己创建的资源，错误时保留有价值日志。不覆盖旧run，不删除父目录或已有仓库/表。
7. 初次和每次feature长构建、truth/集群基准前，说明预计时长、资源与失败风险，取得用户确认后执行。已批准的负载外集群瞬时异常最多重试3次，每次保留证据和新run。

## 4. 子卡与开发顺序

| 卡 | 工作 | 前置/说明 |
|---|---|---|
| #3255 | 10M基线与可重复runner | 必须先完成，不能用旧R3代替 |
| #3256 | cache hit/miss、driver规划与归并归因 | 复用#3089；独立开发可与基线准备并行，集成验收等基线 |
| #3257 | 向量TVF自动列裁剪 | 与观测各worktree并行开发；旧镜像基线不混入新实现 |
| #3258 | 全局选优后取payload | 依赖3256/3257；只延迟payload，保留local refine候选 |
| #3259 | batch预算/分块与query归属 | 依赖3258；兼顾重复open与内存，不能将分块当既有batch收益 |
| #3260 | Dataset lease、轻量规划、locality | 依赖3259；版本/身份/凭证与引用计数安全 |
| #3261 | 高k有界全局归并 | 依赖3260；先证实际Window/TakeOrdered/spill瓶颈 |
| #3262 | core14 FTS语义与namespace观测 | 基线后可并行准备，集群验证仍独占 |
| #3263 | ranked residual有界执行 | 依赖3262；不能MAX_VALUE直接截k |
| #3264 | local分组fragments共享native scan/scorer | 依赖3263；仅减task不够，必须减scanner初始化 |
| #3265 | 共享全局统计的分布式FTS | 依赖3264；必要core桥接先评审，不独立IDF混排 |
| #3266 | hybrid候选融合后去重回表 | 依赖3258/3262；可独立开发，逐feature测量 |

开发的可并行不代表测量可并行。2026-11-25用户明确继续主线并允许subagent并行后，#3256/#3257从基线开发阻塞中解出，在各自worktree做TDD；原始基线仍只运行业务b102cc7的固定镜像，产品feature合入与效果验收等待#3255基线完成。每次只集成一个影响性能的大feature，完成审查、真实smoke、五组回归后再集成下一个，以免多个变更混合导致不能归因。必要细化子卡时保持上述合同，不把未解依赖的卡强行claim。

## 5. 每张feature的完成条件

- 实现前写会失败的关键bug/约束测试，真实Lance数据，不靠mock存储；秒级到十几秒suite优先，并发框架，卡住超过10分钟审查必要性。
- 展示红绿证据、最终diff、相关版本编译/测试、smoke和资源自清理；没有收益的代码清理不包装成性能feature。
- 与基线相同query/truth、snapshot、索引与资源；答案/recall合同不变。优化需要新算法/精排语义时单独提案，不伪装无损性能改动。
- 真实集群smoke使用对应新产物；源码测试通过不等于服务/镜像已生效。
- 五组报告保存相对原始baseline和前一个feature。允许出现负收益或失败，必须解释并决定回退/按成本选择；不拿理论行数削减直接当耗时倍数。
- 设计/方案/决策/最终评审文档保存于仓库docs并提交，同原文upsert-doc逐字节双存SHA256；原始日志、测试脚本、构建物不双存。

## 6. 当前状态

已建立伞卡和全部子卡，并开始#3255准备工作。11月25日只读检查发现远端有其他benchmark相关pod，不满足独占窗口；旧10M truth只有top10，候选批量镜像也缺成功smoke证据。以上是需要在基线阶段解决的前置事实，不是已完成的成绩。暂未运行新集群基准，也未改产品搜索行为。

只读核验的候选资产：纯Lance表version56、586fragments；`r3-ivf-4b`为32段IVF_RQ，首UUID `cab0a8b5-2894-4b4b-819d-9a7b7b821452`，末UUID `9d3cb946-9090-4fb3-a0ff-fa6dcc392d72`；完整段清单与覆盖仍由正式preflight冻结。100query parquet为100行、qid0..99唯一、2560维，part SHA256 `dd29359aecf3fdef8de7ba3dbb21743257851eb72c99514b459c7aa15d4c7f63`。此后若数据/索引变动则必须pin原版本，不能继续以latest比较。

执行前时间预算：复用镜像的编译/核验约2–5分钟，preflight约1–3分钟，小N的exact+batch pilot约5–10分钟、整pilot上限15分钟，首阶段总体约10–20分钟。缺失的100×top2000完整truth预计30–90分钟、硬上限120分钟；五组single合计通常25–45分钟，batch100×k100通常15–30分钟、batch100×k2000通常40–90分钟。全部预计1.5–4小时，独占窗口等待另计；pilot后按实测修订，不承诺旧wide batch大k一定成功。单action15分钟预警/30分钟取消、整batch格120分钟硬停为待确认上限。上述均待用户确认后才执行。
