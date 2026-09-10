# 验证基线与性能测量

本页只维护当前验收基线、计时口径和已结束试验的结论。
规则见[验收规范](截面数据验证匹配规则.md)，执行方法见[使用手册](使用手册.md)。

## 当前版本与覆盖范围

最新全量验收对应提交 `33bf3bb` 的生产源码，Cargo 包版本为 `0.1.0`。
源码在提交前冻结，不能只看 manifest 中的基础 HEAD；源码快照和二进制指纹共同标识版本。
本次文档清理不修改 Rust，也不代表创建发布 tag。冻结 profiling 二进制 SHA256：

```text
27df1c8a2cd902606a218e4bb6b1951ffb9aa9234f00ac57a25dcbe6b6db03d6
```

验收批次：`reports/20260909-p1-borrowed-lookup-full-regression/`。
2026-09-09 北京时间 19:38:19–20:56:50 完成；15 个交易日、沪深全部支持股票和 ETF，
共 30 个日市场任务，最多六进程：

```text
20260127 20260205 20260225 20260311 20260320
20260323 20260401 20260414 20260527 20260601
20260616 20260706 20260806 20260813 20260828
```

| 市场 | 匹配项 | 状态排除 | 不匹配 | 数据错误／缺源 |
| --- | ---: | ---: | ---: | ---: |
| SH | 205,758,141 | 259 | 0 | 0／0 |
| SZ | 208,956,007 | 294 | 0 | 0／0 |
| 合计 | 414,714,148 | 553 | 0 | 0／0 |

30/30 任务通过，完整语义报告及四项 profiling 工作计数与上一轮 allocation 基线逐任务相等。
75 个输入文件的路径、行数、Schema、大小和修改时间与基线一致；这不是全文件内容哈希核验。
原始逐笔和参考 raw snapshot 均读取 `/hdd/data/stock/raw_level2_parquet`，参考价量不参与恢复。

验收边界：

- 使用默认匹配窗口，无诊断扩窗；深市市价单使用 `RestAtLastTradePrice`。
  不代表 `RequireEvidence` 严格待决策略通过，不能与 `is_standard_acceptance()` 混为一谈。
- 状态排除不计入匹配；覆盖不证明所有日期的最大延迟，也不允许失败后自动扩窗。
- 周期 snapshot 匹配不单独证明订单身份、全部逐订单余量、FIFO 或市价单所有中间态。
- 报告省略成功逐帧明细；报告相等不代表逐条核对所有首次命中时间。
  核心单元、差分、属性、固定 golden 与合成 Parquet 测试补充覆盖，CI 不依赖本机 `/hdd`。

## 最近一轮性能比较

对照为 `reports/20260909-p1-allocation-full-regression/` 的冻结版本
（SHA256 `a095c7f0d52116c68c16f9fd04390a520aa57bc71de013d249c594d992dba765`），
不是直接与更早提交比较。两轮均使用相同十五日、六进程配置，不写固定间隔截面。
下表为 30 个任务的累计分项时间，不是并发批次跨度：

| 分项 | 上一轮 | 当前基线 | 耗时变化 |
| --- | ---: | ---: | ---: |
| 输入读取与分片 | 4,722.69 s | 4,635.93 s | 减少 1.84% |
| 回放，不含验证回调 | 10,942.07 s | 10,914.75 s | 减少 0.25% |
| 恢复合计，含分片清理 | 15,830.92 s | 15,733.54 s | 减少 0.62% |
| validation 合计 | 9,707.17 s | 9,776.16 s | 增加 0.71% |
| 恢复 + validation | 25,538.09 s | 25,509.70 s | 减少 0.11% |

每日单市场平均时间：

| 市场 | 恢复：旧 → 新 | validation：旧 → 新 | 合计：旧 → 新 |
| --- | ---: | ---: | ---: |
| SH | 417.34 → 411.08 s | 324.67 → 321.86 s | 742.00 → 732.95 s |
| SZ | 638.06 → 637.82 s | 322.48 → 329.88 s | 960.54 → 967.70 s |

输入分片有 25/30 任务变快，纯回放为 14/30，恢复及端到端各为 18/30。
平均峰值 RSS 为 14.4449 → 14.4394 GiB，基本不变。
六进程批次跨度为 79.69 → 78.52 分钟；调度重叠和尾部任务影响跨度，不能代替累计延时收益。

结论：正确性回归通过，输入阶段改善方向较一致，但没有证实明显、稳定的端到端提速。
分项及逐日结果见该批次 `restore-comparison.json`、`comparison.json`、`*-full-run.json`。
`audit.ipynb` 从原始报告与 timing 文件独立复算；其代码单元已用 Clara Python 顺序执行，
不是通过 Jupyter kernel 执行。所有断言通过。

## 已结束的后续优化试验

基于 `33bf3bb` 的隔离试验位于 `reports/20260909-p1-feasibility/`。
20260828 沪深各 16 个股票和 ETF，按 baseline/spool/report/report/spool/baseline 串行运行，
随后各做一次十档探针，共 14 次；每个普通变体每市场只有两次测量。
14/14 报告和四项处理计数一致。去重范围为 8,956,427 个成功事件、150,825 个匹配项、
2 个状态排除项，不是原型的全市场验收。

| 方向 | 证据与结论 |
| --- | --- |
| 整条记录编解码 | 固定数组写入微基准增加约 30%–38%；真实样本总耗时 SH 减少 0.53%、SZ 增加 1.55%。不采用当前读写整体原型。 |
| 十档缓存细分 | 单侧重复构造约 SH 55%、SZ 41%，但安全失效维护的净成本未测。暂缓，不承诺提速。 |
| 成功报告按需构造 | 报告阶段减少约 SH 54%、SZ 36%。按全量阶段权重条件外推约省 0.47%，不是全市场实测收益。 |

2026-09-10 决定：三项均不再单独投入优化工作；第三项以后整理报告代码时可顺手处理。
实验原型未合并进生产代码。探针包含额外时钟和状态保存开销，不能把它的耗时作为优化结果。
样本总耗时被全市场扫描及负载波动影响，不能把局部收益直接外推为全量提速。
原始日志、构建指纹和 `summary.json` 保留；一次性入口已从 `analysis/` 清理。

## Profiling

验证回调在恢复过程中执行，不能另跑 replay 后将两次总时间相减作为精确对比耗时。
默认生产 API 不记录运行耗时；启用 `profiling` 后，`profile_validate_market_day`
返回同一次验证的分项时间：

```bash
cargo build --release --locked --features profiling --example validation_benchmark
target/release/examples/validation_benchmark \
  --date 20260320 --market SZ --temp-root target/profile-spool \
  --report reports/manual-profile-sz.json \
  --timings reports/manual-profile-sz-timings.json
```

此 example 与 CLI 的证券默认值不同：省略 `--symbols` 表示全部支持的股票与 ETF。
深市使用实用市价策略，不提供扩窗开关。`--max-detail-records` 默认 5000，0 表示不限；
不保留成功逐帧明细，汇总计数不受明细上限影响。使用新路径，不覆盖已有证据。

| 分项 | 范围 |
| --- | --- |
| `reference_load_seconds` | raw snapshot 读取、阶段检查和验证器初始化 |
| `input_spool_seconds` | 逐笔读取、Schema、过滤、通道分片及逆序修复 |
| `replay_excluding_validation_seconds` | 回放循环耗时扣除本次实际验证回调 |
| `validation_callbacks_seconds` | 候选盘口取值、字段比较和收盘审计 |
| `spool_cleanup_seconds` | 成功分片清理 |
| `report_finalize_seconds` | 验证汇总和报告构造 |
| `report_serialization_write_seconds` | 调用者写 JSON，位于核心计时之外 |
| `observation_calls` | profiling observer 的实际观察次数 |
| `scalar_rejected_candidates` | 标量差异界限允许跳过十档读取的候选数 |
| `depth_materializations` | 实际构造十档用于比较的候选数，包括收盘投影 |
| `candidate_cache_hits` | 相同盘口修订号的普通视图复用次数；仍分别比较每条参考帧 |

```text
恢复 = input_spool + replay_excluding_validation + spool_cleanup
对比 = reference_load + validation_callbacks + report_finalize
profiled_total = 恢复 + 对比 + unattributed
```

这些是包含调度、资源争用和埋点成本的 wall time，不是纯 OrderBook::apply CPU 时间。
外层 `/usr/bin/time -v` 还包括启动、JSON 写出和退出，不对埋点开销作未经实测的扣除。
失败时不返回伪造的完整分项。累计耗时代表工作量，不等于并发批次的日历耗时；
不同硬件、缓存或并发负载下不能承诺固定加速比。

## 历史证据与恢复

历史批次按各自源码和二进制存证，不因文档整理变为当前版本证据：

| 阶段 | 版本／关系 | 本地 reports 子目录 |
| --- | --- | --- |
| 初始十五日 | `f3f77c0`，历史整理前为 `73adb97` | `20260907-current-full-regression/`、`20260908-additional-random-full/` |
| Legacy 接口移除 | 仅 20260828 两市回归，不增加独立日期数 | `20260908-legacy-retirement/` |
| P0 | `347027b` | `20260909-p0-optimized-full-regression/` |
| 初轮 P1 | `72b7d43` | `20260909-p1-optimized-full-regression/` |
| 回调优化 | `0b4c37a` | `20260909-p1-callback-full-regression/` |
| 分配／查表 | 提交前冻结，后续包含于 `33bf3bb` | `20260909-p1-allocation-full-regression/` |

`reports/` 由 Git 忽略，克隆仓库不包含输入、详细报告或冻结二进制。
需要从独立备份取得，或使用匹配的源码及输入重新生成；本文摘要不能代替原始证据。
当前工具及依赖见 [analysis/README.md](../analysis/README.md)。

清理前的完整目录（含未提交内容）位于 `reports/20260910-analysis-docs-cleanup/`。
旧阶段日志及退休迁移文档还可从 Git 历史追溯；不改写各报告内的不可变源码快照。
后续若更换基线，必须记录代码／输入指纹、覆盖范围、失败／排除计数和计时环境。
