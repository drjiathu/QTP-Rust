# 验证基线与性能测量

本页集中记录已验证版本、覆盖范围和计时口径；不是验收规则副本。
规则见[验收规范](snapshot-validation-rules.md)，执行方法见[使用手册](Guidance.md)。

## 固定版本与覆盖范围

截至 2026-09-08，历史十五日全量基线为代码提交 `73adb97`，Cargo 包版本 `0.1.0`。
这是验证基线，不代表已创建 release tag。两批使用同一冻结 profiling 二进制，SHA256：

```text
9989b2cc307c518438a03731226d23644a7d1a391a6c4e16ee5a5ff3bc23d335
```

首批在提交前保存源码快照和工作树差异，随后代码提交为上述版本；第二批核对指纹一致。
不能只引用首批 manifest 的基础 HEAD 而忽略源码快照，也不能默认后续改动继承这些结果。

15 个交易日均完成沪深全部支持股票和 ETF 的逐笔恢复及 raw snapshot 验证：

- 原七日：20260320、20260401、20260601、20260616、20260706、20260806、20260828。
- 首轮随机：20260323、20260414、20260527。
- 第二轮随机：20260127、20260205、20260225、20260311、20260813。

| 批次 | 日市场任务 | 匹配项 | 不匹配 | 状态排除 | 数据错误／缺源 |
| --- | ---: | ---: | ---: | ---: | ---: |
| [十日回归](../reports/20260907-current-full-regression/summary.md) | 20 | 277,909,184 | 0 | 381 | 0／0 |
| [五日扩展](../reports/20260908-additional-random-full/summary.md) | 10 | 136,804,964 | 0 | 172 | 0／0 |

共 414,714,148 个可比项全部匹配，553 个状态排除项不算匹配。
股票／ETF、三个阶段的分类、排除原因、输入清单、执行收据及指纹保留在[报告目录](../reports/README.md)。

输入为 `/hdd/data/stock/raw_level2_parquet`，具体逐笔和 raw snapshot 数据集见
[使用手册](Guidance.md#输入与证券范围)。参考价量不参与订单簿恢复。
两批均采用当前默认窗口、无诊断覆盖；深市市价单使用 RestAtLastTradePrice，
不等于 RequireEvidence 严格待决策略通过，不能与 `is_standard_acceptance()` 混为一谈。

## 当前状态与证据边界

旧 QTP legacy 接口已移除，生产恢复／验证算法未改。golden 已拆成两个独立场景文件，
事件序列和预期值不变。上述十五日结果属于接口移除前版本；移除后的回归独立存证如下。

当前已通过 123 项全部 target／feature 测试（golden 从一项拆为两项）、严格 Clippy、
格式、文档测试与 API 文档构建，以及使用手册的核心 Rust 示例。
C++ oracle 及其 CI 检查已移除，固定预期值和 Rust 回归覆盖保留。
生产恢复／验证源文件和 OrderBook 实现字节未变；移除的是旧输入接口而不是算法策略。

20260828 沪深股票与 ETF 全量重跑已于 2026-09-08 完成，两个市场均通过，
结果与移除前同日基线无差异。独立冻结二进制 SHA256 为
`25e9f31abbfd6cd2ba830b0020733f2e50a85a006fabb9f4c795946d59fd6e11`；
[完整结果](../reports/20260908-legacy-retirement/summary.json)和
[版本清单](../reports/20260908-legacy-retirement/manifest.json)保留结果及来源。

| 市场 | 匹配项 | 不匹配 | 状态排除 | 数据错误／缺源 | 恢复秒 | 对比秒 | 核心计时总秒 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SH | 12,342,471 | 0 | 5 | 0／0 | 616.72 | 4,855.62 | 5,472.33 |
| SZ | 13,881,870 | 0 | 18 | 0／0 | 2,025.48 | 4,384.91 | 6,410.39 |

该次为两进程并发，计时按下文 profiling 口径，核心计时不含报告序列化写出。
共 26,224,341 个可比项匹配、23 个状态排除项；同日重跑不增加十五日基线的独立日期数，
也不混加到上表旧二进制的匹配总数。当前 `src` 文件与该次冻结源码逐文件一致；
后续 golden 重构和文档整理未修改生产源码，测试源码则以当前 Rust 检查为准。

- 已知沪市停牌选帧／历史 ETF 收盘阶段、深市委托逆序、V0 后限价单隐藏、
  无涨跌幅限制 E0 展示范围和 ETF 盘中窗口问题均在上述范围内闭合。
- 七份[紧凑诊断](../reports/diagnostics/README.md)保留原因证据；旧中间错误不代表当前失败。
  当前实现及仍存在的市价单中间态风险见[实现说明](order-book-implementation.md)。
- 跨日通过支持当前数据源窗口，不证明所有日期的最大延迟，不允许失败后自动扩窗。
  静态／周期 snapshot 通过不单独证明订单身份、全部逐订单余量和 FIFO。
- 仍保留核心单元、属性、固定 golden 和合成 Parquet 测试；CI 不依赖本机 `/hdd`。
  文档整理本身不扩大已验证范围；不同二进制的验收结果按上述批次分别追溯。

## Profiling

验证回调在恢复过程中执行，不能另跑 replay 后将两次总时间相减作为精确对比耗时。
默认生产 API 不记录运行耗时；启用 `profiling` 后，
`profile_validate_market_day` 返回同一次验证的分项时间：

```bash
cargo build --release --locked --features profiling --example validation_benchmark
target/release/examples/validation_benchmark \
  --date 20260320 --market SZ --temp-root target/profile-spool \
  --report reports/manual-profile-sz.json \
  --timings reports/manual-profile-sz-timings.json
```

此 example 与 CLI 的证券默认值不同：省略 `--symbols` 表示全部支持的股票与 ETF。
深市使用实用市价策略，不提供扩窗开关。`--max-detail-records` 默认 5000，0 表示不限；
不保留成功逐帧明细，全部汇总计数不受明细上限影响。请使用新的报告路径，不覆盖已有证据。

| 分项 | 范围 |
| --- | --- |
| `reference_load_seconds` | raw snapshot 读取、阶段检查和验证器初始化 |
| `input_spool_seconds` | 逐笔读取、Schema、过滤、通道分片及逆序修复 |
| `replay_excluding_validation_seconds` | 回放循环耗时扣除本次实际验证回调 |
| `validation_callbacks_seconds` | 候选盘口取值、字段比较和收盘审计 |
| `spool_cleanup_seconds` | 成功分片清理 |
| `report_finalize_seconds` | 验证汇总和报告构造 |
| `report_serialization_write_seconds` | benchmark 调用者写验证 JSON，位于核心计时之外 |

```text
恢复 = input_spool + replay_excluding_validation + spool_cleanup
对比 = reference_load + validation_callbacks + report_finalize
profiled_total = 恢复 + 对比 + unattributed
```

恢复不是纯 OrderBook::apply CPU 时间；这些都是 wall time，包含调度和资源争用。
时钟埋点有开销，不做未经实测的扣除。外层 `/usr/bin/time -v` 还包含进程启动、报告／
timing JSON 写出和退出；失败时不返回伪造的完整分项，保留进程耗时及失败上下文。

两批最多六进程并发，逐日恢复、对比、总耗时和峰值内存见各 summary。一天两市的跨度
取最早启动至最晚结束，不等于两市耗时相加；累计耗时是工作量，不能当作并发日历耗时。
数据规模、并发负载、缓存和埋点不同，不能直接归因于算法性能提升。

## 证据维护

当前调度／汇总入口见 [analysis/README.md](../analysis/README.md)。manifest、收据、源码
快照与 summary 配套保存；历史报告不因文档或代码更新自动变为新版本证据。
替换基线必须明确代码／输入指纹、覆盖范围、失败与排除计数及计时环境。

早期调查留在[归档](archive/real-data-validation-20260828-investigation.md)，
旧报告清理与恢复说明见 [reports/README.md](../reports/README.md)。
