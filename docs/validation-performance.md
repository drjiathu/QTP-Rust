# 验证耗时的测量口径

## 为什么不能直接把两次运行相减

`validate` 在逐事件恢复过程中调用验证器，对候选盘口取值并比较；它不是先恢复全日、
再独立验证的两个串行程序。另跑一次 `replay` 后用总时间相减，会混入缓存、I/O、并发
争用和不同观察器的影响，不能当作精确的比较耗时。

## 可选 benchmark 埋点

默认生产功能不记录 elapsed 字段。启用 `profiling` feature 后，可通过
`profile_validate_market_day` 和单独的 benchmark 可执行文件测量同一次验证中的时间归属：

```sh
cargo build --release --locked --features profiling --example validation_benchmark
target/release/examples/validation_benchmark \
  --date 20260320 --market SZ \
  --temp-root target/profile-spool \
  --report reports/example-sz.json \
  --timings reports/example-sz-timings.json
```

省略 `--symbols` 时选择全部支持的股票和 ETF；有明确小样本需求时可传逗号分隔代码。
benchmark 沿用当前规范窗口，不提供放宽窗口开关；深市使用 RestAtLastTradePrice，
沪市规则不变。新增合成测试逐字段比较埋点与普通验证报告，并检查错误输入仍然失败。
请使用新的报告路径，示例中的报告路径会被写入；输入 raw Parquet 只读。

| 分项 | 测量范围 |
|---|---|
| `reference_load_seconds` | raw snapshot 读取、阶段检查及验证器初始化 |
| `input_spool_seconds` | 逐笔读取、Schema 检查、过滤、临时通道分片及相关阶段输入 |
| `replay_excluding_validation_seconds` | 回放循环总耗时扣除本次实际验证回调耗时 |
| `validation_callbacks_seconds` | 候选盘口提取、逐字段比较、收盘价审计等验证回调 |
| `spool_cleanup_seconds` | 成功分片清理 |
| `report_finalize_seconds` | 汇总计数、明细和报告构造 |
| `report_serialization_write_seconds` | 调用者序列化、写验证 JSON |

恢复总时间 = input_spool + replay_excluding_validation + spool_cleanup。

对比总时间 = reference_load + validation_callbacks + report_finalize。

两者加 `unattributed_seconds` 等于 `profiled_total_seconds`。外层 `/usr/bin/time -v`
还包括程序启动、写报告、写 timing JSON 和退出，故进程总耗时不要求只等于前两者。

这里的“恢复”含迭代、分片解码、归一化、订单引用解析和簿更新，不是纯 `OrderBook::apply`
CPU 时间。验证回调也是 wall time，不是 CPU 时间；多进程竞争资源和被系统调度暂停均
可能体现在数值中。埋点有时钟读取开销，部分计入恢复流程，不做未经实测的扣除。
失败回放不返回伪造的完整分项耗时；保留进程耗时和失败上下文。

## 跨日期汇总

2026-09-07 修正沪市停牌参考帧分类后的[六日独立全量重跑](../reports/20260907-sh-phase-revalidation/README.md)
使用六个沪市进程并发，全部记录分项耗时。实时/最终结果以该批次 `summary.md` 为准；
未完成任务不得沿用下述旧批次结果。新批次同时检查正常匹配数量、回放统计及原异常分类。

[当前全量汇总](../reports/20260906-random-profiled-full/campaign.md) 合并 20260828、
20260601、20260706、20260806，以及固定种子补抽的 20260320、20260401、20260616。
每日期两个市场；样本不计入。新三日使用埋点版本，旧四日没有埋点的分项标为未记录。

每天两市任务跨度取最早启动到最晚结束，可能包含错峰等待；不是两市耗时相加。
“市场耗时累计”和“恢复／对比累计”是工作量统计，不能当作并发后的日历耗时。
日期、输入规模、并发负载以及是否埋点不同，不能仅据这些耗时判断算法性能提升。

调度与最终汇总分别有持久化日志和心跳。所有任务结束后 `campaign.md` / `campaign.json`
自动刷新最终状态；若心跳过期则报告调度异常，不因旧 `running` 字段误认任务仍在执行。
手动只读核验：`python3 analysis/summarize_validation_campaign.py --require-complete`。
